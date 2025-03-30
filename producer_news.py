from kafka import KafkaProducer
from constants import *
import json


def ini_producer():
    """ Make Kafka producer
    """
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)
    return producer
    

def send_producer(producer, topic_name: str, row_news):    
    """ Publish messages to a topic.
    """
    producer.send(topic_name, key=None, value=str(row_news).encode('utf-8'))


def flush_producer(producer, list_news):
    """ Makes all buffered records immediately available to send
    """
    producer.flush()
    print(f"Data send to Kafka successfully: \n\t{list_news}")
        