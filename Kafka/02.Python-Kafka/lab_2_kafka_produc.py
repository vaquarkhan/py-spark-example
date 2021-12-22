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


bootstrap_servers = ['34.64.77.61:9092'] # kafka broker ip
topicName = 'test-v3'
producer = KafkaProducer(bootstrap_servers = bootstrap_servers)

for i, _ in enumerate(range(30000)):

    # test1 - send numeric type
    print(i)
    producer.send(topicName, str(i).encode())

    # test2 = send string type
    text = 'This is ' + str(i) + ' msg'
    time.sleep(1)
    print(text)

    tim = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    producer.send(topicName, text.encode())
    producer.send(topicName, tim.encode())
