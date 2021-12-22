import json
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# 파이프라인 옵션지정 
pipeline_options = PipelineOptions(None)
DEST_DIR = "gs://freud-int-200423/"
options = {
    'project' : 'freud-int-200423',
    'region' : 'us-central1',
    'staging_location' : DEST_DIR + 'staging',
    'temp_location' : DEST_DIR + 'tmp',
    'job_name' : 'bq-transfer-v2'
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

# 파이프라인 실행
def run(argv=None):
    with beam.Pipeline('DataflowRunner',options=opts) as p:
        from apache_beam.io.gcp.internal.clients import bigquery
        query = """
            SELECT x, y
            FROM `iot_table.iot_data_geo`
            where 1=1
        """

        data_from_source = ( p | 'QueryTable' >> beam.io.Read(beam.io.BigQuerySource(query=query, use_standard_sql=True))
        )

        project_id = 'freud-int-200423'
        dataset_id = 'neogames'
        table_id = 'test_v3'
        table_schema = 'x:float , y:float'

        write_bigquery = ( data_from_source | 'write bigquery' >>  beam.io.WriteToBigQuery(
                                                    project=project_id,
                                                    dataset=dataset_id,
                                                    table=table_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
        )

    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()