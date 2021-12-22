from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'email': ['freud@mz.co.kr'],
    'depends_on_past': False,
    
}

with DAG(
    dag_id='example_databricks_operator',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=days_ago(2),
    tags=['example'],
) as dag:
    new_cluster = {
        'spark_version': '2.1.0-db3-scala2.11',
        'node_type_id': 'r3.xlarge',
        'aws_attributes': {'availability': 'ON_DEMAND'},
        'num_workers': 8,
    }

    notebook_task_params = {
        'new_cluster': new_cluster,
        'notebook_task': {
            'notebook_path': '/Users/freud@mz.co.kr/PrepareData',
        },
    }
    # [START howto_operator_databricks_json]
    # Example of using the JSON parameter to initialize the operator.
    notebook_task = DatabricksSubmitRunOperator(task_id='notebook_task', json=notebook_task_params)
    # [END howto_operator_databricks_json]

    # [START howto_operator_databricks_named]
    # Example of using the named parameters of DatabricksSubmitRunOperator
    # to initialize the operator.
    spark_jar_task = DatabricksSubmitRunOperator(
        task_id='spark_jar_task',
        new_cluster=new_cluster,
        spark_jar_task={'main_class_name': 'com.example.ProcessData'},
        libraries=[{'jar': 'dbfs:/lib/etl-0.1.jar'}],
    )
    # [END howto_operator_databricks_named]
    notebook_task >> spark_jar_task