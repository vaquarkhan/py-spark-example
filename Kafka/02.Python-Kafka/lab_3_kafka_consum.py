from kafka import KafkaConsumer
import json

consumer = KafkaConsumer('order',bootstrap_servers=['3.36.76.207:9092'])
# for message in consumer:
#     print(message.value)


for message in consumer:
    # json_string = json.loads(message)
    print(message)