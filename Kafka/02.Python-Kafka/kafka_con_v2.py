from kafka import KafkaConsumer
import sys
import time

bootstrap_servers = ["35.209.78.194:9092"]
topicName = 'topic-v1'


consumer = KafkaConsumer (topicName, group_id = 'console-consumer-98667',bootstrap_servers = bootstrap_servers,
auto_offset_reset = 'earliest')

 

try:
    for message in consumer:
        # print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,message.offset, message.key,message.value))
        print(message.value)
        time.sleep(1)
except KeyboardInterrupt:
    sys.exit()