
import os
import re
import logging
import argparse
import apache_beam as beam
# from apache_beam.options.pipeline_options import PipelineOptions
# from apache_beam.io import ReadFromText

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:\gcloud_key/freud-int-200423-owner-88790c68f84a.json"

def parse_pubsub(line):
    import json
    test = line.decode(encoding ='utf8')
    record = json.loads(test)
    return record

def run(argv=None):
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input_topic', required=True,
      help='Input PubSub topic of the form "/topics/<PROJECT>/<TOPIC>".')

  known_args, pipeline_args = parser.parse_known_args(argv)


  with beam.Pipeline(argv=pipeline_args) as p:
    data_from_source = ( p | 'input pubsub' >> beam.io.ReadFromPubSub(known_args.input_topic)
                           | 'Write' >> beam.Map(parse_pubsub)
                           #| 'Time_window' >> beam.windowed_value(60, 10)
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