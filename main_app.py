import json
import threading
import uuid
import subprocess
import os
from kafka import KafkaProducer, KafkaConsumer
from time import sleep
from MultiArmBandit import start_predicting, generate_hb


KAFKA_HOST = 'localhost:9092'



def launch_services():
    """
    Lanuches Zookeeper and Kafka services locally
    :return:
    """
    FNULL = open(os.devnull, 'w') #redirect logs to DEVNULL (no logs)
    os.environ['JAVA_HOME'] = '/home/alon/jvm/jdk1.8' #set JAVA_HOME env
    subprocess.call(['sh', './launch_services.sh'], env=os.environ, stdout=FNULL, stderr=subprocess.STDOUT)


def start_producing():
    """
    connects to Kafka server and sends a stream of generated hb events
    :return:
    """
    producer = KafkaProducer(bootstrap_servers=KAFKA_HOST)

    while True:
        message_id = str(uuid.uuid4()) # generate unique message_id
        hb = generate_hb()
        message_body = json.dumps({'hb': hb})
        message = {'request_id': message_id, 'message_body': json.loads(message_body)}

        producer.send('hb_stream', json.dumps(message).encode('utf-8'))
        producer.flush()

        print("-- PRODUCER: Sent message with id {} and body {}".format(message_id, message_body))
        sleep(1)


def start_consuming():
    """

    :return:
    """
    consumer = KafkaConsumer('floors', bootstrap_servers=KAFKA_HOST)

    for msg in consumer:
        message = json.loads(msg.value.decode('utf-8'))
        if 'floor_price' in message:
            print("\033[1;32;40m ** CONSUMER: Received prediction {} for batch id {}".format(message['floor_price'], message['batch_id']))





threads = []
t0 = threading.Thread(target=launch_services)
t1 = threading.Thread(target=start_producing)
t2 = threading.Thread(target=start_consuming)
t3 = threading.Thread(target=start_predicting)
threads.append(t0)
threads.append(t1)
threads.append(t2)
threads.append(t3)

t0.start()
sleep(10)
t1.start()
t2.start()
t3.start()


