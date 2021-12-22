from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.operators.bash_operator import BashOperator
# from airflow.contrib.hooks.aws_lambda_hook import AwsLambdaJobOperator
from airflow.providers.amazon.aws.hooks.lambda_function import AwsLambdaHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from datetime import datetime, timedelta
import boto3
import sys
import subprocess
import random


### glue job specific variables
glue_job_start = "korean-air-glue-job-start"
glue_job_branch_v = "korean-air-lambda-call-job-v"
glue_job_branch_w = "korean-air-lambda-call-job-w"
glue_iam_role = "Super-glueJob"
region_name = "ap-northeast-2"
email_recipient = "freud@mz.co.kr"




default_args = {
    'owner': 'freud',
    'start_date': datetime(2021, 10, 7),
    'retry_delay': timedelta(minutes=5),
    'email': email_recipient,
    'email_on_failure': True
}

def aws_lambda_function(ds,**kwargs):
    hook = AwsLambdaHook('korean-air-lambda-job', 
                             #region_name='ap-norhteast-2',
                             log_type = 'None',
                             #qualifier = '1',
                             invocation_type = 'RequestResponse',
                             config = None,
                             aws_conn_id = 'aws_default'
                        )
    response_1 = hook.invoke_lambda(payload='null')
    print ('Response--->' , response_1)


with DAG(dag_id = 'Korean-Air-Airflow-Only-Lambda-Job', default_args = default_args, schedule_interval = None) as dag:

    lambda_job_call = PythonOperator(
        task_id = 'aws_lambda_function',
        python_callable=aws_lambda_function,
        provide_context=True
    )

    lambda_job_call

