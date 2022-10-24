# -*- coding: utf-8 -*-
import json
from kafka import KafkaProducer
from kafka import KafkaConsumer
import time
def log(str):
    t = time.strftime(r"%Y-%m-%d %H:%M:%S", time.localtime())
    print("[%s]%s" % (t, str))

def kafka_pro():
    producer = KafkaProducer(bootstrap_servers='10.239.14.120:9092')

    msg_dict = {
        "sleep_time": 10,
        "db_config": {
            "database": "test_1",
            "host": "xxxx",
            "user": "root",
            "password": "root"
        },
        "table": "msg",
        "msg": "Hello World"
    }
    msg = json.dumps(msg_dict).encode()
    producer.send('first', msg)
    producer.close()


def kafka_con():
    consumer = KafkaConsumer('first',bootstrap_servers=['10.239.14.120:9092'])
    log('connect')
    log(consumer)
    for msg in consumer:
        log(msg)
        recv = "%s:%d:%d: key=%s value=%s" % (msg.topic, msg.partition, msg.offset, msg.key, msg.value)
        log(recv)
        log('end')
log('start')


kafka_pro()