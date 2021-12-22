
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

src_table_name = "sourceDF"
tablenameInDb = 'log_ga_ods'
batchsize = 3000
partitionsize = 32

## --------------------------------------------
## -- secret 매니저를 이용하여 keystore 사용 할 경우
## --------------------------------------------
client = boto3.client("secretsmanager", region_name="ap-northeast-2")

secret_name = "arn:aws:secretsmanager:ap-northeast-2:020709675181㊙️prod/apdata-cdp-aurmy/mysql-2doNIy"

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

use_sql="""SELECT
    log_seq
    ,log_dttm
    ,site_id
    ,site_nm
    ,google_cid
    ,adid
    ,incs_no
    ,site_mbr_id
    ,incs_yn
    ,login_yn
    ,login_tp_vl
    ,age
    ,yrbr
    ,sex_cd
    ,cust_grd_nm
    ,emp_yn
    ,dvce_tp_cd
    ,pg_url
    ,pg_url_cd20
    ,pg_ttl
    ,pg_nm
    ,pg_loc_vl
    ,pg_tp_cd
    ,pg_hit_sn
    ,pg_cntr_cd
    ,pg_lang_cd
    ,infl_refrer
    ,bounc_yn
    ,log_tp_vl
    ,evnt_cat
    ,evnt_actn
    ,evnt_label
    ,prd_actn_tp
    ,chkout_stp
    ,chkout_optn_vl
    ,pal
    ,ord_no
    ,sr_rslt_list
    ,prd_info
    FROM cdpods.log_ga_ods
    WHERE partitionkey = '2021-10-21'

""".strip('\n').replace('\n','')

## --------------------------------------------
## -- 사용자 정의 SQL
## --------------------------------------------


sourceDF = spark.sql(use_sql)
sourceDF = (sourceDF.withColumn("prd_info", F.concat_ws(",",F.col("prd_info")))
                   .withColumn("sr_rslt_list", F.concat_ws(",",F.col("sr_rslt_list")))
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