from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.dates import days_ago



default_args = {
    'owner': 'airflow',
    'email': ['airflow@example.com'],
    'depends_on_past': False,
}

with DAG(
    dag_id='Databricks_Operator_Run_Now_Job_Task_Four',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=days_ago(2),
    tags=['Airflow_Test'],
) as dag:

    notebook_task_1 = {
        "job_id": 9610,
        "notebook_params": {
            "dry-run": "true",
            "oldest-time-to-consider": "1457570074236"
            }
        }

    notebook_run = DatabricksRunNowOperator(task_id='notebook_run1', json=notebook_task_1)

    notebook_task_2 = {
        "job_id": 9683,
        "notebook_params": {
            "dry-run": "true",
            "oldest-time-to-consider": "1457570074236"
            }
        }

    notebook_run = DatabricksRunNowOperator(task_id='notebook_run2', json=notebook_task_2)

    notebook_task_3 = {
        "job_id": 9957
        }

    notebook_run = DatabricksRunNowOperator(task_id='notebook_run3', json=notebook_task_3)

    notebook_task_4 = {
        "job_id": 9989
        }

    notebook_run = DatabricksRunNowOperator(task_id='notebook_run4', json=notebook_task_4)

notebook_task_1 >> [notebook_task_2 , notebook_task_3] >> notebook_task_4