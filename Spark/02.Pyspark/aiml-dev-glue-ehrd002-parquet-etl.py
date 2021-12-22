import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.gluetypes import *
from awsglue.job import Job
from pyspark.context import SparkContext   
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import HiveContext
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from datetime import datetime


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

today = datetime.today().strftime("%Y-%m-%d")   
year = today.split('-')[0]
month = str(int(today.split('-')[1]) - 1).zfill(2)

if str(int(month) - 1).zfill(2) == '00':
    year = int(year) - 1
    month = '12'
    
# month = '08'

push_down_condition = f"(year == '{year}' and month == '{month}')"

spark.conf.set("spark.sql.execution.arrow.enabled", "true")
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# group by condition
groupByCondition = [
    "macaddress",
    "additionalvalue",
    F.window("registerat", "10 minutes"),
    "year",
    "month",
    "day"
]


# # Partition Key Column Name
partition_name = ['year','month','day']
S3_location = 's3://aiml-dev-event-ehrd-002-etl'

read_catalog = glueContext.create_dynamic_frame.from_catalog(database = "kd_parquet", table_name = "aiml_dev_event_ehrd_002_partition", transformation_ctx = "read_catalog", push_down_predicate = push_down_condition)
pdf2 = read_catalog.toDF()

pdf2_1 = pdf2.select('macaddress','additionalvalue','registerat','ehrdList','year','month','day')

pdf3 = pdf2_1.withColumn("ehrdList",pdf2_1.ehrdList.column[0])

pdf4 = pdf3.withColumn('edata0', F.round(pdf3.ehrdList.getItem(0) % 256).cast('integer')) \
           .withColumn('edata1', F.round(pdf3.ehrdList.getItem(1) / 256).cast('integer')) \
           .withColumn('edata2', F.round(pdf3.ehrdList.getItem(2) % 256).cast('integer')) \
           .withColumn('edata3', F.round(pdf3.ehrdList.getItem(3) / 256).cast('integer')) \
           .withColumn('edata4', F.round(pdf3.ehrdList.getItem(4) % 256).cast('integer')) \
           .withColumn('edata5', F.round(pdf3.ehrdList.getItem(5) / 256).cast('integer')) \
           .withColumn('edata6', F.round(pdf3.ehrdList.getItem(6) % 256).cast('integer')) \
           .withColumn('edata7', F.round(pdf3.ehrdList.getItem(7) / 256).cast('integer')) \
           .withColumn('edata8', F.round(pdf3.ehrdList.getItem(8) % 256).cast('integer')) \
           .withColumn('edata9', F.round(pdf3.ehrdList.getItem(9) / 256).cast('integer')) \
           .withColumn('edata16', F.round(pdf3.ehrdList.getItem(16) % 256).cast('integer')) \
           .withColumn('edata17', F.round(pdf3.ehrdList.getItem(17) / 256).cast('integer'))
           
           
           
pdf5 = pdf4.withColumn("registerat", F.to_timestamp(F.from_unixtime(col("registerat")/F.lit(1000)))).groupBy(groupByCondition).agg(max("edata0").alias("errorcode_low"),max("edata1").alias("errorcode_high"),max("edata2").alias("servicecode_low"),max("edata3").alias("servicecode_high"),max("edata4").alias("errorlevel"),
                                                                                                                                               max("edata5").alias("swversion"),max("edata6").alias("releaseyear_low"),max("edata7").alias("releaseyear_high"),max("edata8").alias("releasedate_low"),max("edata9").alias("releasedate_high"),max("edata16").alias("errortime_low"),max("edata17").alias("errortime_high"))

pdf6 = pdf5.select('*', col('window').start.cast("string").alias("start"), col('window').end.cast("string").alias("end")).drop('window')

           

# dynamic frame transform
applymapping = DynamicFrame.fromDF(pdf6, glueContext , "applymapping")


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






