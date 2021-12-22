import os
import re
import logging
import argparse
import apache_beam as beam
import mysql.connector
from beam_mysql.connector.io import WriteToMySQL
# from apache_beam.options.pipeline_options import PipelineOptions
# from apache_beam.io import ReadFromText

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:\gcloud_key/freud-int-200423-owner-88790c68f84a.json"

# def parse_pubsub(line):
#     import json
#     test = line.decode(encoding ='utf8')
#     record = json.loads(test)
#     return record

def run(argv=None):
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input_topic', required=True,
      help='Input PubSub topic of the form "/topics/<PROJECT>/<TOPIC>".')

  known_args, pipeline_args = parser.parse_known_args(argv)


  with beam.Pipeline(argv=pipeline_args) as p:
    data_from_source = ( p | 'input pubsub' >> beam.io.ReadFromPubSub(known_args.input_topic)
                           | 'decode' >> beam.Map(lambda x: x.decode('utf-8'))
                           | 'Readfromtext' >> beam.io.ReadFromText(x)
                           | 'Parse CSV' >> beam.ParDo(Split(''))
                           
                           #| 'Time_window' >> beam.windowed_value(60, 10)
                        )

    # project_id = 'freud-int-200423'
    # dataset_id = 'youtube'
    # table_id = 'youtube'
    # table_schema = ('comment:STRING')

    data_from_source | WriteToMySQL (
                    host="104.197.237.173",
                    database="test_db",
                    table="tests",
                    user='root',
                    password=1234,
                    port=3306,
                    batch_size=1000
    )

    result = p.run()
    result.wait_until_finish()
                
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()