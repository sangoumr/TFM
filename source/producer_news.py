##
# @package producer_news.py
# # Contains functions to initialize producer, send message to a topic, and make records immediately available.
#
# __More details__
from kafka import KafkaProducer
from TFM.source.constants import BOOTSTRAP_SERVER


def ini_producer():
    """ Make Kafka producer.
    """
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)
    return producer


def send_producer(producer, topic_name: str, row_news):
    """ Publish messages to a topic.

    Args:
        producer (_type_): kafka producer.
        topic_name (str): topic to send msg.
        row_news (_type_): msg to send.
    """
    producer.send(topic_name, key=None, value=str(row_news).encode('utf-8'))


def flush_producer(producer, list_news):
    """ Makes all buffered records immediately available to send

    Args:
        producer (_type_): kafka producer
        list_news (_type_): list of news pending to be available.
    """ 
    producer.flush()
    print(f"Data send to Kafka successfully: \n\t{list_news}")
