#Standard Python Imports
import argparse
import itertools
import logging
import datetime
import time
import base64
import json

#3rd Party Imports
import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
import six
import os
from __future__ import absolute_import


import unittest
import uuid
from builtins import range
from hamcrest.core.core.allof import all_of
from nose.plugins.attrib import attr
from apache_beam.examples.complete.game import leader_board
from apache_beam.io.gcp.tests import utils
from apache_beam.io.gcp.tests.bigquery_matcher import BigqueryMatcher
from apache_beam.runners.runner import PipelineState
from apache_beam.testing import test_utils
from apache_beam.testing.pipeline_verifiers import PipelineStateMatcher
from apache_beam.testing.test_pipeline import TestPipeline

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:\gcloud_key/freud-int-200423-owner-88790c68f84a.json"


def parse_json(line):
    '''Converts line from PubSub back to dictionary
    '''
    record = json.loads(line)
    return record


def decode_message(line):
    '''Decodes the encoded line from Google Pubsub
    '''
    return base64.urlsafe_b64decode(line)


def run(argv=None, save_main_session=True):
    '''Main method for executing the pipeline operation
    '''
    parser = argparse.ArgumentParser()

    parser.add_argument('--input_mode',
                        default='stream',
                        help='Streaming input or file based batch input')

    parser.add_argument('--input_topic',
                        default='projects/freud-int-200423/topics/stream-test-v1',
                        required=True,
                        help='Topic to pull data from.')

    parser.add_argument('--output_table', 
                        required=True,
                        help=
                        ('Output BigQuery table for results specified as: PROJECT:DATASET.TABLE '
                        'or DATASET.TABLE.'))

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend([
      '--project freud-int-200423', 
      '--runner DataflowRunner',
      '--staging_location gs://freud-int-200423/example-20200708/staging', 
      '--temp_location gs://freud-int-200423/example-20200708/temp', 
      '--experiments=allow_non_updatable_job parameter',
      '--input_mode stream', 
      '--input_topic projects/freud-int-200423/topics/stream-test-v1', 
      '--output_table freud-int-200423:stock_test.stock_table'
  ])

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    if known_args.input_mode == 'stream':
        pipeline_options.view_as(StandardOptions).streaming = True

        with beam.Pipeline(options=pipeline_options) as p:
            price = ( p
                | 'ReadInput' >> beam.io.ReadFromPubSub(topic=known_args.input_topic).with_output_types(six.binary_type))
                | 'Decode'  >> beam.Map(decode_message)
                | 'Parse'   >> beam.Map(parse_json) 
                | 'Write to Table' >> beam.io.WriteToBigQuery(
                        known_args.output_table,
                        schema=' timestamp:TIMESTAMP, stock_price:FLOAT',
                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    p.run()