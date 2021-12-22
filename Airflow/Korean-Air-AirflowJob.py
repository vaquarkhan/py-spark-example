from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from airflow.operators.bash_operator import BashOperator
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



def choose_branch(**kwargs):
    branches = ['b1', 'b2']
    chosen = random.choice(branches)
    print(f'chosen: {chosen}')
    return chosen








with DAG(dag_id = 'Korean-Air-Airflow-Glue-Lmabda-Job-Event-1022', default_args = default_args, schedule_interval = None) as dag:

    start_dag = BashOperator(task_id='start', bash_command='echo start')

    # branching = BranchPythonOperator(task_id='choose_branch', python_callable=choose_branch)
    # b1 = BashOperator(task_id='b1', bash_command='echo b1')
    # b2 = BashOperator(task_id='b2', bash_command='echo b2')


    glue_job_branch_v = AwsGlueJobOperator(
        task_id = "glue_job_branch_v",
        job_name = glue_job_branch_v,
        job_desc = f"triggering glue job {glue_job_branch_v}",
        region_name = region_name,
        iam_role_name = glue_iam_role,
        num_of_dpus = 1,
        dag = dag
        )

    glue_job_branch_w = AwsGlueJobOperator(
        task_id = "glue_job_branch_w",
        job_name = glue_job_branch_w,
        job_desc = f"triggering glue job {glue_job_branch_w}",
        region_name = region_name,
        iam_role_name = glue_iam_role,
        num_of_dpus = 1,
        dag = dag
        )

    lambda_job_call = PythonOperator(
        task_id = 'aws_lambda_function',
        python_callable=aws_lambda_function,
        provide_context=True
    )


    # start_dag >> branching >> [b1, b2]
    # b1 >> glue_job_branch_v >> lambda_job_call
    # b2 >> glue_job_branch_w >> lambda_job_call
    start_dag >> glue_job_branch_v >> [lambda_job_call,glue_job_branch_w]
