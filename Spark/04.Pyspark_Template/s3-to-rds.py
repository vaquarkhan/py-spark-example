
import io
import os
import sys
import boto3
import json
import pandas as pd
import time
from awsglue.gluetypes import *
from awsglue.transforms import *
import pyspark.sql.functions as F
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
import datetime



## --------------------------------------------
## -- @params: [JOB_NAME]
## --------------------------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])



sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

from awsglue.context import GlueContext




## --------------------------------------------
## -- 전역변수
## --------------------------------------------

src_table_name = ""
tablenameInDb = ''
batchsize = 3000
partitionsize = 32

## --------------------------------------------
## -- secret 매니저를 이용하여 keystore 사용 할 경우
## --------------------------------------------
client = boto3.client("secretsmanager", region_name="ap-northeast-2")

secret_name = ""

get_secret_value_response = client.get_secret_value(
        SecretId=secret_name
)
secret = get_secret_value_response['SecretString']
secret = json.loads(secret)

db_url = 'jdbc:mysql://' + secret['host'] + ':3368/' + secret['dbname'] + '?characterEncoding=utf8&rewriteBatchedStatements=true'
db_username = secret['username']
db_password = secret['password']


print(db_url)
# &rewriteBatchedStatements=true
#&rewriteBatchedStatements=true


## --------------------------------------------
## -- 사용자 정의 SQL
## --------------------------------------------

use_sql="""

""".strip('\n').replace('\n','')

## --------------------------------------------
## -- 사용자 정의 SQL
## --------------------------------------------


sourceDF = spark.sql(use_sql)
sourceDF = (sourceDF.withColumn("", F.concat_ws(",",F.col("")))
                   .withColumn("", F.concat_ws(",",F.col("")))
                   )



print('df show : ')

sourceDF.show
sourceDF.printSchema()
## --------------------------------------------
## -- datasource 설정
## --------------------------------------------

sourceDF = sourceDF.repartition(partitionsize)
print('read parquet')

sourceDF = (sourceDF.withColumn("etl_prod_dttm",F.current_timestamp().cast("timestamp"))
            )


## --------------------------------------------
## -- Connecting to the source  (병렬 JDBC 사용)
## --------------------------------------------

print('write ready')

sourceDF.coalesce(partitionsize).write \
    .format("jdbc") \
    .option("url", db_url) \
    .option("user", db_username) \
    .option("password", db_password) \
    .option("dbtable", tablenameInDb) \
    .option("batchsize",batchsize) \
    .mode("overwrite") \
    .save()



print('write db')


job.commit()
