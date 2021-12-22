# #!/app/python/bin/python

# from kafka import KafkaConsumer

# consumer = KafkaConsumer('test-v1',bootstrap_servers=['35.184.251.184:9092'])
# for message in consumer:
#     print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
#                 message.offset, message.key,
#                 message.value))

import time
import random
import datetime
from kafka import KafkaProducer


bootstrap_servers = ['35.192.138.22:9092'] # kafka broker ip
topicName = 'topic-v1'
producer = KafkaProducer(bootstrap_servers = bootstrap_servers)

for i, _ in enumerate(range(300)):

    # test1 - send numeric type
    print(i)
    producer.send(topicName, str(i).encode())

    # test2 = send string type
    text = 'This is ' + str(i) + ' msg'
    print(text)

    tim = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    producer.send(topicName, text.encode())
    producer.send(topicName, tim.encode())

    time.sleep(10)