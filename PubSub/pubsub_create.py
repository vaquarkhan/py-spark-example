from google.cloud import pubsub_v1
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:\gcloud_key/freud-int-200423-owner-88790c68f84a.json"

# TODO(developer)
project_id = "freud-int-200423"
topic_id = "stream-test-v3"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

topic = publisher.create_topic(topic_path)

print("Topic created: {}".format(topic))