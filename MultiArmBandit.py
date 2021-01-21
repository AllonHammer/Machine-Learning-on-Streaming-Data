import json
import numpy as np
import os
import glob
import pandas as pd
import random
from distfit import distfit
from kafka import KafkaProducer, KafkaConsumer

HB_MESSAGE_PATH = './messages/raw_hb'
HB_PARAMS_PATH = './messages/params'
FLOORS = np.append(np.array([0.01]), np.arange(0.1,2.1,0.1))
KAFKA_HOST = 'localhost:9092'
BATCH_SIZE = 20

def start_predicting():
    """
    This function consumes streams of hb data
    and once a batch is accumulated the prediction process starts
    :return:
    """
    clear_messages()
    init_params()
    consumer = KafkaConsumer('hb_stream', bootstrap_servers=KAFKA_HOST)
    batch_id = 0
    message_cnt = 0
    for msg in consumer:
        message = json.loads(msg.value.decode('utf-8'))
        write_hb_to_file(message,  batch_id)
        message_cnt += 1
        if message_cnt % BATCH_SIZE == 0:
            batch_id += 1
            floor = fit()
            publish_prediction(floor, batch_id)


def fit():
    """
    This function consumes one batch of hb, updates the params and decides on the floor price based on MAB algorithm
    :return: float (best action or floor price)
    """
    current_batch = get_latest_batch()
    current_params = read_params()
    f = current_params['current_floor']
    nominator = current_batch[current_batch.hb>=f]['hb'].sum()
    denominator = current_batch.shape[0]
    rcpm_batch = nominator / denominator
    current_params['t'] += 1
    selected_action = MAB(current_params)
    current_params['N_ta'][str(selected_action)] += 1
    current_params['Q_ta'][str(selected_action)] += (1/current_params['N_ta'][str(selected_action)]) * \
                                                    (rcpm_batch - current_params['Q_ta'][str(selected_action)] )
    current_params['current_floor'] = selected_action
    write_params_to_file(current_params)
    return selected_action

def MAB(params):
    """
    selected_action = Argmax {a in A} Qt(a) + np.sqrt ( 2*log(t) / (Nt(a) +1))
    :param params: dict
    :return: float (best action or floor price)
    """
    A = params['A']
    t = params['t']
    best_a = []
    best_value = -999
    for a in A:
        Q_ta = params['Q_ta'][str(a)]
        N_ta = params['N_ta'][str(a)]
        value = Q_ta + np.sqrt(2*np.log(t)/(N_ta+1))
        if value > best_value:
            best_value = value
            best_a = [a]
        elif value == best_value:
            best_a.append(a)
        else:
            continue

    return random.choice(best_a)

def init_params():
    """
    this function init the model's parameters
    :return:
    """
    floors = [np.round(k,2) for k in FLOORS]
    Q_ta = {k:0 for k in floors}
    N_ta = {k:0 for k in floors}
    t = 0
    params = {'t': t, 'A': floors, 'Q_ta': Q_ta, 'N_ta': N_ta, 'current_floor': 0}
    write_params_to_file(params)




def read_params():
    """
    This function reads the latest update of the parameters
    :return: dict
    """
    path = os.path.join(HB_PARAMS_PATH, 'params.txt')
    with open(path, 'r') as f:
        lines = f.read().splitlines()
        last_line = lines[-1]
        d = json.loads(last_line)
    f.close()

    return d





def get_latest_batch():
    """
    This function reads the latest batch of data
    :return: pd.DataFrame
    """
    list_of_files = glob.glob('{}/*.csv'.format(HB_MESSAGE_PATH))
    latest_file = max(list_of_files, key=os.path.getctime)
    df = pd.read_csv(latest_file, header=None)
    df.columns = ['request_id', 'hb']
    return df





def write_hb_to_file(message,  batch_id):
    """
    This function write a single line of ['request_id' ,'hb'] to the correct csv file, according to the batch
    :param message: dict
    :param batch_id: int
    :return:
    """
    message_fname = 'hb_batch_{}.csv'.format(batch_id)
    f = open(os.path.join(HB_MESSAGE_PATH, message_fname), "a")
    line = "{},{}".format(message['request_id'], message['message_body']['hb'])
    f.write("%s\n" % (line))
    f.close()


def write_params_to_file(message):
    """
    There is a single file, each line t contains the parameters at time t. The last line is the most recent update
    This function writes a single line to this file
    :param message: dict
    :return:
    """
    message_fname = 'params.txt'
    f = open(os.path.join(HB_PARAMS_PATH, message_fname), "a")
    f.write("%s\n" % (json.dumps(message)))
    f.close()



def publish_prediction(floor, batch_id):
    """
    This function produces messages to the 'floors' Topic for the main.app consumer
    :param floor: float
    :param batch_id: int
    :return:
    """
    producer = KafkaProducer(bootstrap_servers=KAFKA_HOST)
    producer.send('floors', json.dumps({'batch_id': batch_id, 'floor_price': float(floor)}).encode('utf-8'))
    producer.flush()



def clear_messages():
    files1 = glob.glob('{}/*.csv'.format(HB_MESSAGE_PATH))
    files2 = glob.glob('{}/*.txt'.format(HB_PARAMS_PATH))
    for f in files1 + files2:
        os.remove(f)




def distributions():
    base_scale = 0.705 #1/Lambda
    dists = []
    for i,floor in enumerate(FLOORS):
        scale = base_scale + np.log(1+floor)
        dists.append({'scale': scale})
    return dists

def find_best_dist(dists):
    best = 0
    best_i = -1
    for i,floor in enumerate(FLOORS):
        scale = dists[i]['scale']
        sample = np.random.exponential(scale, 1000000)
        sample = np.clip(sample, 0 ,5)
        above_floor = sample[np.where(sample>floor)]
        nominator = np.sum(above_floor)
        denominator = sample.shape[0]
        rcpm = nominator/denominator
        if rcpm>best:
            best = rcpm
            best_i = i
        dists[i]['ev'] = rcpm
    print('Expected value of distributions')
    print([d['ev'] for d in dists ])
    print('Best Floor price is {}'.format(FLOORS[best_i]))

def dist_fit():
    sample = pd.read_csv('sample_dist.csv')['highest_bid'].values
    dist = distfit(method='parametric', alpha=0.05)
    dist.fit_transform(sample)
    best_distr = dist.model
    print(best_distr)
    print(dist.summary)
    dist.plot_summary()




def generate_hb():
    """ generate hb from correct dist according to floor price.
    in first interation use base dist"""

    floors = [np.round(k,2) for k in FLOORS]
    dists = distributions()
    try:
        current_floor = read_params()['current_floor']
        if current_floor == 0:
            hb = np.random.exponential(0.705)
        else:
            idx = floors.index(current_floor)
            chosen_dist = dists[idx]
            hb = np.random.exponential(chosen_dist['scale'])

    except FileNotFoundError or ValueError: #first iteration
        hb = np.random.exponential(0.705)



    return hb




