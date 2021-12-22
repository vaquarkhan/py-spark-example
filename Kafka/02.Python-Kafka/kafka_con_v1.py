from kafka import KafkaConsumer
import json

consumer = KafkaConsumer('cdc',bootstrap_servers=['34.64.77.61:9092'])
# for message in consumer:
#     print(message.value)


for message in consumer:
    json_string = json.loads(message.value)
    query = json_string['ddl']
    print(query)