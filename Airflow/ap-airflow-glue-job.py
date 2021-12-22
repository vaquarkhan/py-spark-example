from sys import executable
from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import AwsGlueCrawlerOperator
# from StartGlueCrawlerOperator import *
# from airflow.providers.amazon.aws.hooks.glue_crawler import AwsGlueCrawlerHook
from datetime import datetime, timedelta


### glue job specific variables
glue_job_name_to_rds = "airflow-s3-to-rds-glue-job"
glue_job_name_to_s3 = "airflow-rds-to-s3-glue-job"
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


with DAG(dag_id = 'glue_ap_pipeline_add_craw', default_args = default_args, schedule_interval = None) as dag:
    
    s3_to_rds_glue_job_step = AwsGlueJobOperator(
        task_id = "s3_to_rds_glue_job_step",
        job_name = glue_job_name_to_rds,
        job_desc = f"triggering glue job {glue_job_name_to_rds}",
        region_name = region_name,
        iam_role_name = glue_iam_role,
        num_of_dpus = 1,
        dag = dag
        )

    rds_to_s3_glue_job_step = AwsGlueJobOperator(
        task_id = "rds_to_s3_glue_job_step",
        job_name = glue_job_name_to_s3,
        job_desc = f"triggering glue job {glue_job_name_to_s3}",
        region_name = region_name,
        iam_role_name = glue_iam_role,
        num_of_dpus = 1,
        dag = dag
        )

    # glue_crawler_operator = AwsGlueCrawlerOperator(
    #     aws_conn_id="aws_default",
    #     task_id="glue_crawler_operator",
    #     crawler_name="airflow-crawler",
    # )

    glue_crawler_operator = AwsGlueCrawlerOperator(task_id="glue_crawler_operator",
                                                #  aws_conn_id=aws_secret, 
                                                 config={"region":"ap-northeast-2", "role_arn":"arn:aws:iam::190803208249:role/Super-glueJob"},
                                                 crawler_name ="airflow-crawler",
                                                 poll_interval=60, priority_weight=3
                                                 )

    # glue_crawler_operator = StartGlueCrawlerOperator(
    # task_id='glue_crawler_operator',
    # crawler_name='airflow-crawler',
    # polling_interval=10,
    # dag=dag

   
    s3_to_rds_glue_job_step >> rds_to_s3_glue_job_step >> glue_crawler_operator