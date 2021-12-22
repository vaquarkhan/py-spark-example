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
from pyspark.sql.window import Window
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


# # Partition Key Column Name
partition_name = ['year','month','day']

today = datetime.today().strftime("%Y-%m-%d")   
year = today.split('-')[0]
month = str(int(today.split('-')[1]) - 1).zfill(2)

if str(int(month) - 1).zfill(2) == '00':
    year = int(year) - 1
    month = '12'
# month = '08'

push_down_condition = f"(year == '{year}' and month == '{month}')"

S3_location = 's3://aiml-dev-alive-002-etl/aiml-dev-alive-002-etl-result/'

windowSpec = Window.orderBy('macaddress').orderBy('registerat').partitionBy('macaddress')
windowSpec2 = Window.orderBy('macaddress').partitionBy('month')

groupByCondition = [        'macaddress',
                            F.window('registerat', '10 minutes'),
                            'additionalvalue',
                            'rcswversion',
                            'did01bxmcver',
                            'did00bxmcversub',
                            'did21bxboilermodelcode',
                            'did22bxboilertypecode',
                            'did23bxboilercapacode',
                            'txcd09bxindoorctemp',
                            'year',
                            'month',
                            'day' 
]

def lag_col(df,column1,column2,column3,column4,column5):
    df = df.select(*[x for x in df.columns if x not in (column1, column2, column3, column4, column5)] ,
                     F.coalesce((F.col(column1) - (F.lag(F.col(column1),1).over(windowSpec))), F.col(column1)).alias(column1),
                     F.coalesce((F.col(column2) - (F.lag(F.col(column2),1).over(windowSpec))), F.col(column2)).alias(column2),
                     F.coalesce((F.col(column3) - (F.lag(F.col(column3),1).over(windowSpec))), F.col(column3)).alias(column3),
                     F.coalesce((F.col(column4) - (F.lag(F.col(column4),1).over(windowSpec))), F.col(column4)).alias(column4),
                     F.coalesce((F.col(column5) - (F.lag(F.col(column5),1).over(windowSpec))), F.col(column5)).alias(column5))

    return df


def lag_col2(df,column1,column2,column3,column4):
    df = df.select(*[x for x in df.columns if x not in (column1, column2, column3, column4)] ,
                     F.coalesce((F.col(column1) - (F.lag(F.col(column1),1).over(windowSpec))), F.col(column1)).alias(column1),
                     F.coalesce((F.col(column2) - (F.lag(F.col(column2),1).over(windowSpec))), F.col(column2)).alias(column2),
                     F.coalesce((F.col(column3) - (F.lag(F.col(column3),1).over(windowSpec))), F.col(column3)).alias(column3),
                     F.coalesce((F.col(column4) - (F.lag(F.col(column4),1).over(windowSpec))), F.col(column4)).alias(column4))

    return df





# # --------------------------------------------
# # -- START
# # --------------------------------------------

print('----------------------------------start---------------------------------')
# Read from Catalog 



read_catalog = glueContext.create_dynamic_frame.from_catalog(database = 'kd_etl', table_name = 'aiml_dev_alive_002_etl_01', transformation_ctx = "read_catalog", push_down_predicate = push_down_condition)

print('----------------------------------success read catalog----------------------------------')

# Transform to SparkDF
trans_df = read_catalog.toDF()

trans_df.printSchema()


print('----------------------------------trans to DF ----------------------------------')


sort_df = trans_df.orderBy(['macaddress','registerat'])


# 누적값 차이 계산

cacul_lag = lag_col(sort_df,'mtd_8_11','mtd_12_15','mtd_16_19','mtd_20_23','mtd_24_27')


print('----------------------------------lag_task_1----------------------------------')

cacul_lag.printSchema()


# 누적값 차이 계산
cacul_lag2 = lag_col2(cacul_lag,'mtd_0_1','mtd_2_3','mtd_4_5','mtd_6_7')
cacul_lag2.printSchema()


print('----------------------------------lag_task_2_success----------------------------------')




etl_ten_minutes = (cacul_lag2.withColumn('registerat', F.to_timestamp(F.from_unixtime(F.col('registerat')/F.lit(1000)))).groupBy(groupByCondition)
                  .agg(
                        F.ceil(F.avg('tcp0')).cast('integer').alias('tcp0'),
                        F.ceil(F.avg('tcp1')).cast('integer').alias('tcp1'),
                        F.ceil(F.avg('tcp2')).cast('integer').alias('tcp2'),
                        F.ceil(F.avg('tcp3')).cast('integer').alias('tcp3'),
                        F.ceil(F.avg('tcp4')).cast('integer').alias('tcp4'),
                        F.ceil(F.avg('tcp5')).cast('integer').alias('tcp5'),
                        F.ceil(F.avg('tcp6')).cast('integer').alias('tcp6'),
                        F.ceil(F.avg('om0')).cast('integer').alias('om0'),
                        F.ceil(F.avg('om1')).cast('integer').alias('om1'),
                        F.ceil(F.avg('om2')).cast('integer').alias('om2'),
                        F.ceil(F.avg('om3')).cast('integer').alias('om3'),
                        F.ceil(F.avg('om4')).cast('integer').alias('om4'),
                        F.ceil(F.avg('om5')).cast('integer').alias('om5'),
                        F.ceil(F.avg('om6')).cast('integer').alias('om6'),
                        F.ceil(F.avg('om7')).cast('integer').alias('om7'),
                        F.ceil(F.avg('cdm0')).cast('integer').alias('cdm0'),
                        F.ceil(F.avg('cdm1')).cast('integer').alias('cdm1'),
                        F.ceil(F.avg('cdm2')).cast('integer').alias('cdm2'),
                        F.ceil(F.avg('cdm3')).cast('integer').alias('cdm3'),
                        F.ceil(F.avg('cdm4')).cast('integer').alias('cdm4'),
                        F.ceil(F.avg('cdm5')).cast('integer').alias('cdm5'),
                        F.ceil(F.avg('cdm6')).cast('integer').alias('cdm6'),
                        F.ceil(F.avg('cdm7')).cast('integer').alias('cdm7'),
                        F.ceil(F.avg('rd26_0')).cast('integer').alias('rd26_0'),
                        F.ceil(F.avg('rd26_1')).cast('integer').alias('rd26_1'),
                        F.ceil(F.avg('rd26_2')).cast('integer').alias('rd26_2'),
                        F.ceil(F.avg('rd26_3')).cast('integer').alias('rd26_3'),
                        F.ceil(F.avg('rd26_4')).cast('integer').alias('rd26_4'),
                        F.ceil(F.avg('rd26_5')).cast('integer').alias('rd26_5'),
                        F.ceil(F.avg('rd26_6')).cast('integer').alias('rd26_6'),
                        F.ceil(F.avg('rd26_7')).cast('integer').alias('rd26_7'),
                        F.ceil(F.avg('rd27_0')).cast('integer').alias('rd27_0'),
                        F.ceil(F.avg('rd27_1')).cast('integer').alias('rd27_1'),
                        F.ceil(F.avg('rd27_2')).cast('integer').alias('rd27_2'),
                        F.ceil(F.avg('rd27_3')).cast('integer').alias('rd27_3'),
                        F.ceil(F.avg('rd27_4')).cast('integer').alias('rd27_4'),
                        F.ceil(F.avg('rd27_5')).cast('integer').alias('rd27_5'),
                        F.ceil(F.avg('rd27_6')).cast('integer').alias('rd27_6'),
                        F.round(F.avg('rddata4')).alias('rddata4'),
                        F.round(F.avg('rddata6')).alias('rddata6'),
                        F.round(F.avg('rddata7')).alias('rddata7'),
                        F.round(F.avg('rdData10')).alias('rdData10'),
                        F.round(F.avg('rdData13')).alias('rdData13'),
                        F.round(F.avg('rd_18_19')).alias('rd_18_19'),
                        F.round(F.avg('rd_20_21')).alias('rd_20_21'),
                        F.round(F.avg('rd_22_23')).alias('rd_22_23'),
                        F.last('statenum').alias('statenum'),
                        F.round(F.avg('mtd_28_29'), 2).alias('mtd_28_29'),
                        F.round(F.avg('mtd_30_31'), 2).alias('mtd_30_31'),
                        F.round(F.avg('mtd_32_33'), 2).alias('mtd_32_33'),
                        (F.last('mtd_0_1') - F.first('mtd_0_1')).alias('mtd_0_1'),
                        F.when((F.last('mtd_2_3')) == 0,0 ).otherwise((F.last('mtd_2_3') - F.first('mtd_2_3'))).alias('mtd_2_3'),
                        F.when((F.last('mtd_4_5')) == 0,0 ).otherwise((F.last('mtd_4_5') - F.first('mtd_4_5'))).alias('mtd_4_5'),
                        F.when((F.last('mtd_6_7')) == 0,0 ).otherwise((F.last('mtd_6_7') - F.first('mtd_6_7'))).alias('mtd_6_7'),
                        F.when((F.last('mtd_8_11')) == 0,0 ).otherwise((F.last('mtd_8_11') - F.first('mtd_8_11'))).alias('mtd_8_11'),
                        F.when((F.last('mtd_12_15')) == 0,0 ).otherwise((F.last('mtd_12_15') - F.first('mtd_12_15'))).alias('mtd_12_15'),
                        F.when((F.last('mtd_16_19')) == 0,0 ).otherwise((F.last('mtd_16_19') - F.first('mtd_16_19'))).alias('mtd_16_19'),
                        F.when((F.last('mtd_20_23')) == 0,0 ).otherwise((F.last('mtd_20_23') - F.first('mtd_20_23'))).alias('mtd_20_23'),
                        F.when((F.last('mtd_24_27')) == 0,0 ).otherwise((F.last('mtd_24_27') - F.first('mtd_24_27'))).alias('mtd_24_27')
        ))

result_table = etl_ten_minutes.select('*', F.col('window').start.cast('string').alias('start'), F.col('window').end.cast('string').alias('end')).drop('window')


print('----------------------------------result success----------------------------------')


time.sleep(5)

print('----------------------------------s3 write ready----------------------------------')










print('----------------------------------result success----------------------------------')



time.sleep(5)

print('----------------------------------s3 write ready----------------------------------')


# dynamic frame transform
applymapping = DynamicFrame.fromDF(result_table, glueContext , "applymapping")

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
              
print('----------------------------------S3 Write success----------------------------------')
print('----------------------------------Job EXIT----------------------------------')
job.commit()