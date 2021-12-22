import os
import re
import logging
import json
import argparse
import apache_beam as beam
from kafka import KafkaConsumer
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.external import ReadFromKafkaSchema
from apache_beam.transforms.external import kafka
from apache_beam.transforms.external import ReadFromKafka

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:\gcloud_key/freud-int-200423-owner-88790c68f84a.json"

pipeline_options = PipelineOptions(None)
DEST_DIR = "gs://freud-int-200423/"
options = {
    'staging_location': DEST_DIR + 'staging',
    'temp_location': DEST_DIR + 'tmp',
    'job_name': 'kafka-test-v1',
    'project': 'freud-int-200423', #project id
    'region' : 'us-central1-a',
    'teardown_policy': 'TEARDOWN_ALWAYS',
    'no_save_main_session': True ,  
    'save_main_session': False
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)



p = beam.Pipeline('DataflowRunner', options=opts)
data_from_source = ( p | 'input kafka' >> beam.io.ReadFromKafka(consumer_config={'bootstrap.servers':'35.192.138.22:9092'},topics=['topic-v1'])
                       | 'Mapping kafka' >> beam.Map(lambda comment : {'comment': comment})
                    )

project_id = 'freud-int-200423'
dataset_id = 'youtube'
table_id = 'youtube'
table_schema = ('comment:STRING')

data_from_source | beam.io.WriteToBigQuery(
                table=table_id,
                dataset=dataset_id,
                project=project_id,
                schema=table_schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

result = p.run()
result.wait_until_finish()
                
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()