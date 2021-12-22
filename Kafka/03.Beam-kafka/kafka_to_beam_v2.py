import os
import re
import logging
import json
import argparse
from kafka import KafkaConsumer
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.io.kafka import WriteToKafka
# from beam_nuggets.io import kafkaio

# from apache_beam.io.kafka import WriteToKafka
# from apache_beam.io.kafka import ReadFromKafkaSchema
# from apache_beam.io.kafka import WriteToKafkaSchema 



os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:\gcloud_key/freud-int-200423-owner-88790c68f84a.json"

pipeline_options = PipelineOptions(None)
DEST_DIR = "gs://freud-int-200423/"


options = {
    'staging_location': DEST_DIR + 'staging',
    'temp_location': DEST_DIR + 'tmp',
    'job_name': 'kafka-v1',
    'project': 'freud-int-200423', #project id
    'region' : 'us-central1',
    # 'bootstrap_server' : '34.70.151.26:9092',
    'experiments' : 'use_runner_v2',
    # 'num_workers' : 5,
    # 'teardown_policy': 'TEARDOWN_ALWAYS',
    # 'no_save_main_session': True ,  
    # 'save_main_session': False
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

# kafka_topic = "test-v11"
# kafka_config = {"topic" : kafka_topic,
#                  "bootstrap_servers": '34.70.151.26:9092',
#                  "group_id": "notification_consumer_group"}

def decode_kafka(message):
    consumer = KafkaConsumer('test-v13',bootstrap_servers=['34.70.151.26:9092'])
    for message in consumer:
        ka = message.value
        test1 = ka.decode(encoding = 'utf-8')
        test2 = json.loads(test1)
        print(test2)

with beam.Pipeline('DataflowRunner',options=opts) as p:
    pipe = ( p | "Read Kafka" >> beam.Map(decode_kafka)
    # notifications = (p | "Read kafka" >> kafkaio.KafkaConsume(kafka_config)
                    #    | " Write Kafka" >> beam.Map(print)


    # data_from_source = ( p | 'json load' >> beam.Map(parse_kafka)
                        #    | 'input kafka' >> ReadFromKafka(
                        #         consumer_config={'bootstrap.servers': '34.70.151.26:9092'},topics=['test-v10'])
                        #    | 'Read kafka' >> ReadFromKafka(parse_kafka)

    # data_from_source = ( p  | 'Mapping kafka' >> beam.Map(parse_kafka)
    #                         | 'input kafka' >> ReadFromKafka(
    #                             consumer_config={'bootstrap.servers': bootstrap_servers},
    #                             topics=[topic])


    # data_from_source = ( p  | 'Write kafka' >> WriteToKafka(producer_config={'bootstrap.servers' : bootstrap_servers},
    #                                         topic=topic)
    #                         | 'input kafka' >> ReadFromKafka(
    #                             consumer_config={'bootstrap.servers': bootstrap_servers},
    #                             topics=[topic])


    # data_from_source = ( p  | 'Read kafka' >> ReadFromKafka( consumer_config={'bootstrap.servers': '34.70.151.26:9092'},
    #                                                          topics=['test-v10'])
    #                         | 'decode Kafka' >> beam.Map(parse_kafka)
                            
                           
                        
    #                     #    | 'Mapping kafka' >> beam.Map(lambda comment : {'comment': comment})
                        )

    # project_id = 'freud-int-200423'
    # dataset_id = 'kafka'
    # table_id = 'kafka_table_v1'
    # table_schema = ('comment:STRING')

    # data_from_source | beam.io.WriteToBigQuery(
    #                 table=table_id,
    #                 dataset=dataset_id,
    #                 project=project_id,
    #                 schema=table_schema,
    #                 create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    #                 write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

    result = p.run()
    result.wait_until_finish()
                
# if __name__ == '__main__':
#   logging.getLogger().setLevel(logging.INFO)
#   run()
# print(data_from_source)
