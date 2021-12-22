import os
import json
import re
import logging
import argparse
import apache_beam as beam

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:\gcloud_key/freud-int-200423-owner-88790c68f84a.json"

def parse_pubsub(data):
    data_temp = data.decode(encoding = 'utf-8')
    record = json.loads(data_temp)
    return record

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input_topic', required= True,
        help = 'input_topic_name')
    parser.add_argument(
        '--output_table', required=True,
        help = 'output_bq_name')

    known_args, pipeline_args = parser.parse_known_args(argv)

    with beam.Pipeline(argv=pipeline_args) as p:
        lines = ( p | 'input topic' >> beam.io.ReadFromPubSub(known_args.input_topic)
                    | 'mapping pubsub' >> beam.Map(parse_pubsub)
                    | 'write bq' >> beam.io.WriteToBigQuery(
                      known_args.output_table,
                      schema='stock_price:FLOAT , timestamp:TIMESTAMP',
                      create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                      write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)      
                )
        

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
