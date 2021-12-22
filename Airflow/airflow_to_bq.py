import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow import models
from airflow.utils import trigger_rule
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
import importlib



	
default_args = {
    'owner': 'freud-int-200423',
    'start_date': datetime(2020, 10, 21),
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 0,
    'retry_delay' : timedelta(minutes=5),
    'project_id': 'freud-int-200423'
}

query1 = """
    select x,y
    from `freud-int-200423.iot_table.iot_data_geo`
    where 1=1
    """

query2 = """
    select y
    from `freud-int-200423.iot_table.iot_data_geo`
    where 1=1
    """

with models.DAG(
    dag_id = 'test-v1',
    schedule_interval= '*/5 * * * *',
    default_args=default_args) as dag:
    
        bq_query1 = BigQueryOperator(
            dag = dag,
            task_id='extract_daily_metric1',
            sql=query1,
            bigquery_conn_id='bigquery_default',
            use_legacy_sql=False,
            destination_dataset_table='neogames.airflow1',
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_APPEND'
            )
        bq_query2 = BigQueryOperator(
            dag = dag,
            task_id='extract_daily_metric2',
            sql=query2,
            bigquery_conn_id='bigquery_default', 
            use_legacy_sql=False,
            destination_dataset_table='neogames.airflow2',
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_APPEND'
            )
	
bq_query1 >> bq_query2

