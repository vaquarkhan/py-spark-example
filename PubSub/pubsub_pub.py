from google.cloud import pubsub_v1
#import os
import time
import json


#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:\gcloud_key/freud-int-200423-owner-88790c68f84a.json"

project_id = "freud-int-200423"
topic_id = "test-v1"

publisher = pubsub_v1.PublisherClient()
topic = 'projects/{}/topics/{}'.format(project_id, topic_id)

for n in range(1, 10000):
    body = {
                "number" : n,
                "option" : n*2
            }
    json_body = json.dumps(body)
    data = bytes(json_body, 'utf8')
    message_future = publisher.publish(
        topic, 
        data=data,
        )
    time.sleep(1)
    print(data)