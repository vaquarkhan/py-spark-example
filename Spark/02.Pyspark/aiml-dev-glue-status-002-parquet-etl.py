import sys
import time
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from awsglue.gluetypes import *
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import HiveContext
from pyspark.sql.column import Column, _to_java_column, _to_seq, _create_column_from_literal
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from datetime import datetime

# # --------------------------------------------
# # -- @params: [JOB_NAME]
# # --------------------------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)

# set custom logging on
logger = glueContext.get_logger()
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# # --------------------------------------------
# # -- 전역변수
# # --------------------------------------------

today = datetime.today().strftime("%Y-%m-%d")   
year = today.split('-')[0]
month = str(int(today.split('-')[1]) - 1).zfill(2)

if str(int(month) - 1).zfill(2) == '00':
    year = int(year) - 1
    month = '12'
# month = '08'


sc = spark.sparkContext

groupByCondition = [
    "macaddress",
    "additionalvalue",
    F.window("registerat", "10 minutes"),
    "operationmode",
    "year", # for partition
    "month",
    "day"
]

push_down_condition = f"(year == '{year}' and month == '{month}')"

# # Partition Key Column Name
partition_name = ['year','month','day']
S3_location = 's3://aiml-dev-response-status-002-etl/'

read_catalog = glueContext.create_dynamic_frame.from_catalog(database="kd_parquet", table_name="aiml_dev_response_status_002_partition", transformation_ctx = "read_catalog", push_down_predicate = push_down_condition)
pdf2 = read_catalog.toDF()

df = pdf2.withColumn("registerat", F.to_timestamp(F.from_unixtime(col("registerat")/F.lit(1000)))).groupBy(groupByCondition).agg(round(avg("insidetemperature"), 2).alias("insidetemperature"), round(avg("hotwatertemperaturesetting"), 2).alias("hotwatertemperaturesetting"), round(avg("insidetemperaturesetting"), 2).alias("insidetemperaturesetting"), F.max("operationBusy").alias("operationbusy"), F.max("ondolTemperatureSetting").alias("ondoltemperaturesetting"), F.max("errorcode").alias("errorcode_status"))
df = df.select('*', col('window').start.cast("string").alias("start"), col('window').end.cast("string").alias("end")).drop('window')

for column in df.columns:
    start_index = column.find('(')
    end_index = column.find(')')
    if start_index > -1 and end_index > -1:
        df = df.withColumnRenamed(column, column[start_index+1:end_index])

applymapping = DynamicFrame.fromDF(df, glueContext , "applymapping")

# write S3
datasink = glueContext.write_dynamic_frame_from_options(
              frame=applymapping,
              connection_type="s3",
              connection_options={
                  "path": S3_location,
                  "partitionKeys": partition_name
              },
              format="glueparquet",
              format_options = {"compression": "snappy"},
              transformation_ctx ="datasink")
job.commit()