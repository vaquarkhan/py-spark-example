# -*- Encoding:UTF-8 -*- #
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import os
import time
import json
import base64

project_id = "freud-int-200423"
subscription_id = "yun-test-v1"

# topic_id = "pub_to_bq_v1"

timeout = 15
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:\gcloud_key/freud-int-200423-owner-88790c68f84a.json"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message):
    # message_json = json.loads(Message.data)
    data = message.data
    # data1 = json.dumps(data,ensure_ascii=False)
    # test1 = test.decode(encoding = 'UTF-8')
    jsonload = data.decode(encoding='utf-8')
    # jsonload1 = json.loads(jsonload,encoding='utf-8')
    # data1 = json.dumps(data,ensure_ascii=False)
    # data2 = json.loads(data1)
    print(jsonload)
    time.sleep(10)
    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
time.sleep(15)
print("Listening for messages on {}..\n".format(subscription_path))

with subscriber:
    try:
        streaming_pull_future.result(timeout=timeout)    
    except TimeoutError:
        streaming_pull_future.cancel()