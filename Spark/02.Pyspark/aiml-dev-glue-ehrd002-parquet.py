import sys
import boto3
import json
import pyspark
from datetime import datetime
import pyspark.sql.functions as F
from awsglue.gluetypes import *
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job


# # --------------------------------------------
# # -- @params: [JOB_NAME]
# # --------------------------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# # --------------------------------------------
# # -- 전역변수
# # --------------------------------------------


# # Partition Key Column Name
partition_name = ['Year','Month','Day']

today = datetime.today().strftime("%Y-%m-%d")   
year = today.split('-')[0]
month = str(int(today.split('-')[1]) - 1).zfill(2)

if str(int(month) - 1).zfill(2) == '00':
    year = int(year) - 1
    month = '12'
    
# month = '08'

push_down_condition = f"(partition_1 == 'year-{year}' and partition_2 == 'month-{month}')"

raw_data = glueContext.create_dynamic_frame.from_catalog(database = "kd_raw", table_name = "aiml_dev_event_ehrd_002", transformation_ctx = "raw_data" , push_down_predicate = push_down_condition)
S3_location = 's3://aiml-dev-event-ehrd-002-partition/'


#convert to spark DataFrame 
sparkDF = raw_data.toDF()

partitioningDF = sparkDF.withColumn('Year', F.split(sparkDF['partition_1'], '-').getItem(1)) \
                        .withColumn('Month', F.split(sparkDF['partition_2'], '-').getItem(1)) \
                        .withColumn('Day', F.split(sparkDF['partition_3'], '-').getItem(1))


partdropDF = partitioningDF.drop('partition_0').drop('partition_1').drop('partition_2').drop('partition_3')

resultDF = DynamicFrame.fromDF(partdropDF, glueContext , "resultDF")




datasink = glueContext.write_dynamic_frame_from_options(
              frame=resultDF,
              connection_type="s3",
              connection_options={
                  "path": S3_location,
                  "partitionKeys": partition_name
              },
              format="glueparquet",
              format_options = {"compression": "snappy"},
              transformation_ctx ="datasink")

job.commit()
