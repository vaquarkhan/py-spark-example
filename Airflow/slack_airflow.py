import airflow
from airflow import DAG
from airflow import models
from airflow.operators import bash_operator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.slack_operator import SlackAPIPostOperator
from datetime import datetime, timedelta




args = {
    'owner': 'freud-int-200423',
    'start_date': datetime(2020, 12, 2),
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 0,
    'retry_delay' : timedelta(minutes=5),
    'project_id': 'freud-int-200423'
}



with models.DAG(
        dag_id='example_multi_gcs_to_bq_operator_v3', 
        default_args=args,
        schedule_interval='*/1 * * * *') as dag:
            t = datetime.today().strftime("%Y_%m_%d_%H_%M")
            load_json = GoogleCloudStorageToBigQueryOperator(
                task_id='multigcs_to_bq_example_v3',
                bucket='freud-int-200423',
                source_objects=['airflow_test/gcs_test_{}.json'.format(t)],
                destination_project_dataset_table='mc_test.multi_get_test_v1',
                source_format='NEWLINE_DELIMITED_JSON',
                write_disposition='WRITE_APPEND',
                dag=dag
                )

