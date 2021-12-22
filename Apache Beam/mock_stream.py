import time
import datetime
import json
import base64
import os

from StockGenerator import *
from google.cloud import pubsub

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:\gcloud_key/freud-int-200423-owner-88790c68f84a.json"

PROJECT = 'freud-int-200423'
TOPIC = 'stock_option'

def pub_callback(message_future):
    # When timeout is unspecified, the exception method waits indefinitely.
    topic = 'projects/{}/topics/{}'.format(PROJECT, TOPIC)
    if message_future.exception(timeout=30):
        print('Publishing message on {} threw an Exception {}.'.format(
            topic, message_future.exception()))
    else:
        print(message_future.result())

def main():
    publisher = pubsub.PublisherClient()
    topic = 'projects/{}/topics/{}'.format(PROJECT, TOPIC)
    stock_price = StockGenerator(mu = 1.001, sigma = 0.001, starting_price = 100)

    while True:

        time.sleep(1)
        price = next(stock_price)
        timestamp = str(datetime.datetime.utcnow()) # str to make json serializable

        body = {
            'stock_price': price,
            'timestamp': timestamp,
        }

        str_body = json.dumps(body)
        print(str_body)
        
        data = bytes(str_body, 'utf8')
        message_future = publisher.publish(
            topic, 
            data=data,
            )
        message_future.add_done_callback(pub_callback)

if __name__ == '__main__':
    main()