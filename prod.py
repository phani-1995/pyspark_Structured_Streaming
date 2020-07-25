#Importing necessary modules
from time import sleep
from kafka import KafkaProducer
from alpha_vantage.timeseries import TimeSeries
import random
import json
from json import dumps
def get_data():
    ticker = 'GOOGL'
    lines = open('alpha_ventage').read().splitlines()
    keys = random.choice(lines)
    time = TimeSeries(key=keys, output_format='json')
    data, metadata = time.get_intraday(symbol=ticker, interval='1min', outputsize='full')
    return data

#Publishing message
def publish_message(producerkey,key,data_key):
    key_bytes = bytes(key, encoding='utf-8')
    producerkey.send("stock", json.dumps(data[key]).encode('utf-8'), key_bytes)

    print("message published")

 # value_serializer=lambda x:
 #                         dumps(x).encode('utf-8'))
#Connecting to kafka
def producer_connection():
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    return producer
#
# if __name__ == "main":
#     data = get_data()
#     if len(data) > 0:
#         kafka_producer = producer_connection()
#
#         for key in sorted(data):
#             publish_message(kafka_producer,key,data[key])
#             sleep(3)

if __name__=="__main__":
    data = get_data()
    if len(data) > 0:
        kafka_producer = producer_connection()
        for key in sorted(data):
            publish_message( kafka_producer,key, data[key])
            sleep(3)