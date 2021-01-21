from kafka import KafkaProducer, KafkaConsumer
import threading
import uuid
import json
KAFKA_HOST = 'localhost:9092'

def p():
    producer = KafkaProducer(bootstrap_servers=KAFKA_HOST)
    for i in range(101,200):
        message_id = str(uuid.uuid4())  # generate unique message_id
        message_body = json.dumps({'hb': i})
        message = {'request_id': message_id, 'message_body': json.loads(message_body)}

        #producer.send('test_topic', str(i).encode('utf-8'))
        producer.send('test_topic', json.dumps(message).encode('utf-8'))

        print('sent ', str(i))
        producer.flush()

def c():
    consumer = KafkaConsumer('test_topic', bootstrap_servers=KAFKA_HOST, auto_offset_reset='earliest', group_id=None)
    for msg in consumer:
        print('received ', msg.value)




threads = []
t0 = threading.Thread(target=p)
t1 = threading.Thread(target=c)
threads.append(t0)
threads.append(t1)

t0.start()
t1.start()
