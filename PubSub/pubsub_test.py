from google.cloud import pubsub_v1
import os
import time
import json
import base64
import byte
from collections import OrderedDict


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:\gcloud_key/freud-int-200423-owner-88790c68f84a.json"

project_id = "freud-int-200423"
topic_id = "stream-number-v2"

publisher = pubsub_v1.PublisherClient()
# topic_path = publisher.topic_path(project_id, topic_id)
topic = 'projects/{}/topics/{}'.format(project_id, topic_id)

for n in range(1, 100):
    number = n
    body = {
        "message" : [
            {
                "number" : n
            }
        ]
    }
    json_body = json.dumps(body)
    # str_body = body
    # print(str_body)
    data = base64.urlsafe_b64encode(bytes(json_body, 'utf8'))
    # data = base64.b64encode(str_body)
    message_future = publisher.publish(
        topic, 
        data=data,
        )
    # message_future.add_done_callback(pub_callback)
    # future = publisher.publish(
    #     topic_path, str_body, origin="python-sample", username="gcp")
    time.sleep(1)
    print(type(json_body))
    print(type(data))
    print(json_body)
    print(data)
    # print(type(message_future))