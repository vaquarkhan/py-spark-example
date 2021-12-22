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


S3_location = 's3://aiml-dev-alive-002-etl/aiml-dev-alive-002-etl-01/'

windowSpec = Window.orderBy('macaddress').orderBy('registerat').partitionBy('macaddress')
windowSpec2 = Window.partitionBy(['macaddress','month']).orderBy(F.col('registerat').asc())
windowSpec3 = Window.partitionBy(['macaddress','month']).orderBy(F.col('registerat').desc())


# # --------------------------------------------
# # -- UDF 및 함수
# # --------------------------------------------

def hex(col):
    jc = sc._jvm.functions.hex(_to_java_column(col))
    return Column(jc)
    
    
def bin(col):
    jc = sc._jvm.functions.bin(_to_java_column(col))
    return Column(jc)


@udf('String')
def hex_to_int(x):
    func = lambda x : int(x,16)
    return func(x)



def col_bin(df,column1,column2,column3,column4,column5):
    df = (df.select(*[x for x in df.columns if x not in (column1, column2, column3, column4, column5)],
            bin(F.col(column1)).cast('integer').alias(column1),
            bin(F.col(column2)).cast('integer').alias(column2),
            bin(F.col(column3)).cast('integer').alias(column3),
            bin(F.col(column4)).cast('integer').alias(column4),
            bin(F.col(column5)).cast('integer').alias(column5) 
    ))
    
    return df




# # --------------------------------------------
# # -- START
# # --------------------------------------------

print('----------------------------------start---------------------------------')
# Read from Catalog 

read_catalog = glueContext.create_dynamic_frame.from_catalog(database = 'kd_parquet', table_name = 'aiml_dev_alive_002_partition', transformation_ctx = "read_catalog" , push_down_predicate = push_down_condition)

print('----------------------------------success read catalog----------------------------------')

# Transform to SparkDF
trans_df = read_catalog.toDF()

trans_df.printSchema()


print('----------------------------------trans to DF ----------------------------------')

trans_df2 = trans_df.select('macaddress',
                            'registerat',
                            'additionalvalue',
                            'rcswversion',
                            'did01bxmcver',
                            'did00bxmcversub',
                            'did21bxboilermodelcode',
                            'did22bxboilertypecode',
                            'did23bxboilercapacode',
                            'tcprcmode',
                            'txcd09bxindoorctemp',
                            'rddata0',
                            'rddata1',
                            'rddata2',
                            'rddata3',
                            'rddata4',
                            'rddata6',
                            'rddata7',
                            'rddata10',
                            'rddata13',
                            'rddata18',
                            'rddata19',
                            'rddata20',
                            'rddata21',
                            'rddata22',
                            'rddata23',
                            'rddata26',
                            'rddata27',
                            'mtddata0',
                            'mtddata1',
                            'mtddata2',
                            'mtddata3',
                            'mtddata4',
                            'mtddata5',
                            'mtddata6',
                            'mtddata7',
                            'mtddata8',
                            'mtddata9',
                            'mtddata10',
                            'mtddata11',
                            'mtddata12',
                            'mtddata13',
                            'mtddata14',
                            'mtddata15',
                            'mtddata16',
                            'mtddata17',
                            'mtddata18',
                            'mtddata19',
                            'mtddata20',
                            'mtddata21',
                            'mtddata22',
                            'mtddata23',
                            'mtddata24',
                            'mtddata25',
                            'mtddata26',
                            'mtddata27',
                            'mtddata28',
                            'mtddata29',
                            'mtddata30',
                            'mtddata31',
                            'mtddata32',
                            'mtddata33',
                            'year',
                            'month',
                            'day'  
)           	   


pdf_start = trans_df2.filter((trans_df2.did01bxmcver!= 0) & (trans_df2.did00bxmcversub != 0))


# transform 10 -> 16 -> little endian -> 10


trans_int = (pdf_start.select('*',
        hex_to_int(F.concat(F.lpad(hex(F.col('mtdData11').cast('integer')), 2, '0'),F.lpad(hex(F.col('mtdData10').cast('integer')), 2, '0'),F.lpad(hex(F.col('mtdData9').cast('integer')), 2, '0'),F.lpad(hex(F.col('mtdData8').cast('integer')), 2, '0'))).cast('integer').alias('mtd_8_11'),
        hex_to_int(F.concat(F.lpad(hex(F.col('mtdData15').cast('integer')), 2, '0'),F.lpad(hex(F.col('mtdData14').cast('integer')), 2, '0'),F.lpad(hex(F.col('mtdData13').cast('integer')), 2, '0'),F.lpad(hex(F.col('mtdData12').cast('integer')), 2, '0'))).cast('integer').alias('mtd_12_15'),
        hex_to_int(F.concat(F.lpad(hex(F.col('mtdData19').cast('integer')), 2, '0'),F.lpad(hex(F.col('mtdData18').cast('integer')), 2, '0'),F.lpad(hex(F.col('mtdData17').cast('integer')), 2, '0'),F.lpad(hex(F.col('mtdData16').cast('integer')), 2, '0'))).cast('integer').alias('mtd_16_19'),
        hex_to_int(F.concat(F.lpad(hex(F.col('mtdData23').cast('integer')), 2, '0'),F.lpad(hex(F.col('mtdData22').cast('integer')), 2, '0'),F.lpad(hex(F.col('mtdData21').cast('integer')), 2, '0'),F.lpad(hex(F.col('mtdData20').cast('integer')), 2, '0'))).cast('integer').alias('mtd_20_23'),
        hex_to_int(F.concat(F.lpad(hex(F.col('mtdData27').cast('integer')), 2, '0'),F.lpad(hex(F.col('mtdData26').cast('integer')), 2, '0'),F.lpad(hex(F.col('mtdData25').cast('integer')), 2, '0'),F.lpad(hex(F.col('mtdData24').cast('integer')), 2, '0'))).cast('integer').alias('mtd_24_27')
    )).drop('mtdData8','mtdData9','mtdData10','mtdData11','mtdData12','mtdData13','mtdData14','mtdData15','mtdData16','mtdData17','mtdData18','mtdData19','mtdData20','mtdData21','mtdData22','mtdData23','mtdData24','mtdData25','mtdData26','mtdData27')





print('----------------------------------trans_int success----------------------------------')

       
trans_int2 = (trans_int.select('*',
                    hex_to_int(F.concat(F.lpad(hex(F.col('rdData19').cast('integer')), 2, '0'),F.lpad(hex(F.col('rdData18').cast('integer')), 2, '0'))).cast('integer').alias('rd_18_19'),
                    hex_to_int(F.concat(F.lpad(hex(F.col('rdData21').cast('integer')), 2, '0'),F.lpad(hex(F.col('rdData20').cast('integer')), 2, '0'))).cast('integer').alias('rd_20_21'),
                    hex_to_int(F.concat(F.lpad(hex(F.col('rdData23').cast('integer')), 2, '0'),F.lpad(hex(F.col('rdData22').cast('integer')), 2, '0'))).cast('integer').alias('rd_22_23'),
                    hex_to_int(F.concat(F.lpad(hex(F.col('mtdData29').cast('integer')), 2, '0'),F.lpad(hex(F.col('mtdData28').cast('integer')), 2, '0'))).cast('integer').alias('mtd_28_29'),
                    hex_to_int(F.concat(F.lpad(hex(F.col('mtdData31').cast('integer')), 2, '0'),F.lpad(hex(F.col('mtdData30').cast('integer')), 2, '0'))).cast('integer').alias('mtd_30_31'),
                    hex_to_int(F.concat(F.lpad(hex(F.col('mtdData33').cast('integer')), 2, '0'),F.lpad(hex(F.col('mtdData32').cast('integer')), 2, '0'))).cast('integer').alias('mtd_32_33')
            )).drop('rdData18','rdData19','rdData20','rdData21','rdData22','rdData23','mtdData28','mtdData29','mtdData30','mtdData31','mtdData32','mtdData33')



print('----------------------------------trans_int2 success----------------------------------')

       
trans_int3 = (trans_int2.select('*',
                    hex_to_int(F.concat(F.lpad(hex(F.col('mtdData1').cast('integer')), 2, '0'),F.lpad(hex(F.col('mtdData0').cast('integer')), 2, '0'))).cast('integer').alias('mtd_0_1'),
                    hex_to_int(F.concat(F.lpad(hex(F.col('mtdData3').cast('integer')), 2, '0'),F.lpad(hex(F.col('mtdData2').cast('integer')), 2, '0'))).cast('integer').alias('mtd_2_3'),
                    hex_to_int(F.concat(F.lpad(hex(F.col('mtdData5').cast('integer')), 2, '0'),F.lpad(hex(F.col('mtdData4').cast('integer')), 2, '0'))).cast('integer').alias('mtd_4_5'),
                    hex_to_int(F.concat(F.lpad(hex(F.col('mtdData7').cast('integer')), 2, '0'),F.lpad(hex(F.col('mtdData6').cast('integer')), 2, '0'))).cast('integer').alias('mtd_6_7')
    )).drop('mtdData0','mtdData1','mtdData2','mtdData3','mtdData4','mtdData5','mtdData6','mtdData7')


       
       
print('----------------------------------trans_int3 success----------------------------------')
       


## binary

trans_bin = col_bin(trans_int3,'tcprcmode','rddata0','rddata1','rddata26','rddata27')

print('----------------------------------trans_binary success----------------------------------')
                        

explode_bin = (trans_bin.select('*',
                        (F.split(F.col('tcprcmode'),'')[0]).alias('tcp0_tmp'),
                        (F.split(F.col('tcprcmode'),'')[1]).alias('tcp1_tmp'),
                        (F.split(F.col('tcprcmode'),'')[2]).alias('tcp2_tmp'),
                        (F.split(F.col('tcprcmode'),'')[3]).alias('tcp3_tmp'),
                        (F.split(F.col('tcprcmode'),'')[4]).alias('tcp4_tmp'),
                        (F.split(F.col('tcprcmode'),'')[5]).alias('tcp5_tmp'),
                        (F.split(F.col('tcprcmode'),'')[6]).alias('tcp6_tmp'),
                        (F.split(F.col('rddata0'),'')[0]).alias('om0_tmp'),
                        (F.split(F.col('rddata0'),'')[1]).alias('om1_tmp'),
                        (F.split(F.col('rddata0'),'')[2]).alias('om2_tmp'),
                        (F.split(F.col('rddata0'),'')[3]).alias('om3_tmp'),
                        (F.split(F.col('rddata0'),'')[4]).alias('om4_tmp'),
                        (F.split(F.col('rddata0'),'')[5]).alias('om5_tmp'),
                        (F.split(F.col('rddata0'),'')[6]).alias('om6_tmp'),
                        (F.split(F.col('rddata0'),'')[7]).alias('om7_tmp'),
                        (F.split(F.col('rddata1'),'')[0]).alias('cdm0_tmp'),
                        (F.split(F.col('rddata1'),'')[1]).alias('cdm1_tmp'),
                        (F.split(F.col('rddata1'),'')[2]).alias('cdm2_tmp'),
                        (F.split(F.col('rddata1'),'')[3]).alias('cdm3_tmp'),
                        (F.split(F.col('rddata1'),'')[4]).alias('cdm4_tmp'),
                        (F.split(F.col('rddata1'),'')[5]).alias('cdm5_tmp'),
                        (F.split(F.col('rddata1'),'')[6]).alias('cdm6_tmp'),
                        (F.split(F.col('rddata1'),'')[7]).alias('cdm7_tmp'),
                        (F.split(F.col('rddata26'),'')[0]).alias('rd26_0_tmp'),
                        (F.split(F.col('rddata26'),'')[1]).alias('rd26_1_tmp'),
                        (F.split(F.col('rddata26'),'')[2]).alias('rd26_2_tmp'),
                        (F.split(F.col('rddata26'),'')[3]).alias('rd26_3_tmp'),
                        (F.split(F.col('rddata26'),'')[4]).alias('rd26_4_tmp'),
                        (F.split(F.col('rddata26'),'')[5]).alias('rd26_5_tmp'),
                        (F.split(F.col('rddata26'),'')[6]).alias('rd26_6_tmp'),
                        (F.split(F.col('rddata26'),'')[7]).alias('rd26_7_tmp'),
                        (F.split(F.col('rddata27'),'')[0]).alias('rd27_0_tmp'),
                        (F.split(F.col('rddata27'),'')[1]).alias('rd27_1_tmp'),
                        (F.split(F.col('rddata27'),'')[2]).alias('rd27_2_tmp'),
                        (F.split(F.col('rddata27'),'')[3]).alias('rd27_3_tmp'),
                        (F.split(F.col('rddata27'),'')[4]).alias('rd27_4_tmp'),
                        (F.split(F.col('rddata27'),'')[5]).alias('rd27_5_tmp'),
                        (F.split(F.col('rddata27'),'')[6]).alias('rd27_6_tmp')
        )).drop('tcprcmode','rddata0','rddata1','rddata26','rddata27').replace("","0").na.fill("0")

explode_bin_int = (explode_bin.select('*',
                            F.col('tcp0_tmp').cast('integer').alias('tcp0'),
                            F.col('tcp1_tmp').cast('integer').alias('tcp1'),
                            F.col('tcp2_tmp').cast('integer').alias('tcp2'),
                            F.col('tcp3_tmp').cast('integer').alias('tcp3'),
                            F.col('tcp4_tmp').cast('integer').alias('tcp4'),
                            F.col('tcp5_tmp').cast('integer').alias('tcp5'),
                            F.col('tcp6_tmp').cast('integer').alias('tcp6'),
                            F.col('om0_tmp').cast('integer').alias('om0'),
                            F.col('om1_tmp').cast('integer').alias('om1'),
                            F.col('om2_tmp').cast('integer').alias('om2'),
                            F.col('om3_tmp').cast('integer').alias('om3'),
                            F.col('om4_tmp').cast('integer').alias('om4'),
                            F.col('om5_tmp').cast('integer').alias('om5'),
                            F.col('om6_tmp').cast('integer').alias('om6'),
                            F.col('om7_tmp').cast('integer').alias('om7'),
                            F.col('cdm0_tmp').cast('integer').alias('cdm0'),
                            F.col('cdm1_tmp').cast('integer').alias('cdm1'),
                            F.col('cdm2_tmp').cast('integer').alias('cdm2'),
                            F.col('cdm3_tmp').cast('integer').alias('cdm3'),
                            F.col('cdm4_tmp').cast('integer').alias('cdm4'),
                            F.col('cdm5_tmp').cast('integer').alias('cdm5'),
                            F.col('cdm6_tmp').cast('integer').alias('cdm6'),
                            F.col('cdm7_tmp').cast('integer').alias('cdm7'),
                            F.col('rd26_0_tmp').cast('integer').alias('rd26_0'),
                            F.col('rd26_1_tmp').cast('integer').alias('rd26_1'),
                            F.col('rd26_2_tmp').cast('integer').alias('rd26_2'),
                            F.col('rd26_3_tmp').cast('integer').alias('rd26_3'),
                            F.col('rd26_4_tmp').cast('integer').alias('rd26_4'),
                            F.col('rd26_5_tmp').cast('integer').alias('rd26_5'),
                            F.col('rd26_6_tmp').cast('integer').alias('rd26_6'),
                            F.col('rd26_7_tmp').cast('integer').alias('rd26_7'),
                            F.col('rd27_0_tmp').cast('integer').alias('rd27_0'),
                            F.col('rd27_1_tmp').cast('integer').alias('rd27_1'),
                            F.col('rd27_2_tmp').cast('integer').alias('rd27_2'),
                            F.col('rd27_3_tmp').cast('integer').alias('rd27_3'),
                            F.col('rd27_4_tmp').cast('integer').alias('rd27_4'),
                            F.col('rd27_5_tmp').cast('integer').alias('rd27_5'),
                            F.col('rd27_6_tmp').cast('integer').alias('rd27_6')
      )).drop('tcp0_tmp').drop('tcp1_tmp').drop('tcp2_tmp').drop('tcp3_tmp').drop('tcp4_tmp').drop('tcp5_tmp').drop('tcp6_tmp').drop('om0_tmp').drop('om1_tmp').drop('om2_tmp').drop('om3_tmp').drop('om4_tmp').drop('om5_tmp').drop('om6_tmp').drop('om7_tmp') \
        .drop('cdm0_tmp').drop('cdm1_tmp').drop('cdm2_tmp').drop('cdm3_tmp').drop('cdm4_tmp').drop('cdm5_tmp').drop('cdm6_tmp').drop('cdm7_tmp') \
        .drop('rd26_0_tmp').drop('rd26_1_tmp').drop('rd26_2_tmp').drop('rd26_3_tmp').drop('rd26_4_tmp').drop('rd26_5_tmp').drop('rd26_6_tmp').drop('rd26_7_tmp') \
        .drop('rd27_0_tmp').drop('rd27_1_tmp').drop('rd27_2_tmp').drop('rd27_3_tmp').drop('rd27_4_tmp').drop('rd27_5_tmp').drop('rd27_6_tmp')

print('----------------------------------explode success----------------------------------')

## alive state ETL
labeling_1 = (explode_bin_int.withColumn('statenum',(F.when((F.col('rdData2') < 20),0))
                                                    .when((F.col('rdData2') > 19) & (F.col('rdData2') < 31) ,1)
                                                    .when((F.col('rdData2') > 30) & (F.col('rdData2') < 46) ,2)
                                                    .when((F.col('rdData2') > 50) & (F.col('rdData2') < 60) ,3)
                                                    .when((F.col('rdData2') > 59) & (F.col('rdData2') < 80) ,4))
          ).drop('rdData2')

print('----------------------------------labeling success----------------------------------')



column_etl_1 = (labeling_1.select(*[x for x in labeling_1.columns if x not in ('mtd_0_1')], 
                (F.when((F.col('mtd_0_1') > F.first('mtd_0_1',True).over(windowSpec3))|
                (F.col('mtd_0_1') < F.first('mtd_0_1',True).over(windowSpec2)),
                F.lag('mtd_0_1').over(windowSpec2)).otherwise(F.col('mtd_0_1'))).alias('mtd_0_1')))

column_etl_2 = (column_etl_1.select(*[x for x in column_etl_1.columns if x not in ('mtd_2_3')], 
                (F.when((F.col('mtd_2_3') > F.first('mtd_2_3',True).over(windowSpec3))|
                (F.col('mtd_2_3') < F.first('mtd_2_3',True).over(windowSpec2)),
                F.lag('mtd_2_3').over(windowSpec2)).otherwise(F.col('mtd_2_3'))).alias('mtd_2_3')))

column_etl_3 = (column_etl_2.select(*[x for x in column_etl_2.columns if x not in ('mtd_4_5')], 
                (F.when((F.col('mtd_4_5') > F.first('mtd_4_5',True).over(windowSpec3))|
                (F.col('mtd_4_5') < F.first('mtd_4_5',True).over(windowSpec2)),
                F.lag('mtd_4_5').over(windowSpec2)).otherwise(F.col('mtd_4_5'))).alias('mtd_4_5')))


column_etl_4 = (column_etl_3.select(*[x for x in column_etl_3.columns if x not in ('mtd_6_7')], 
                (F.when((F.col('mtd_6_7') > F.first('mtd_6_7',True).over(windowSpec3))|
                (F.col('mtd_6_7') < F.first('mtd_6_7',True).over(windowSpec2)),
                F.lag('mtd_6_7').over(windowSpec2)).otherwise(F.col('mtd_6_7'))).alias('mtd_6_7')))


column_etl_5 = (column_etl_4.select(*[x for x in column_etl_4.columns if x not in ('mtd_8_11')], 
                (F.when((F.col('mtd_8_11') > F.first('mtd_8_11',True).over(windowSpec3))|
                (F.col('mtd_8_11') < F.first('mtd_8_11',True).over(windowSpec2)),
                F.lag('mtd_8_11').over(windowSpec2)).otherwise(F.col('mtd_8_11'))).alias('mtd_8_11')))


column_etl_6 = (column_etl_5.select(*[x for x in column_etl_5.columns if x not in ('mtd_12_15')], 
                (F.when((F.col('mtd_12_15') > F.first('mtd_12_15',True).over(windowSpec3))|
                (F.col('mtd_12_15') < F.first('mtd_12_15',True).over(windowSpec2)),
                F.lag('mtd_12_15').over(windowSpec2)).otherwise(F.col('mtd_12_15'))).alias('mtd_12_15')))


column_etl_7 = (column_etl_6.select(*[x for x in column_etl_6.columns if x not in ('mtd_16_19')], 
                (F.when((F.col('mtd_16_19') > F.first('mtd_16_19',True).over(windowSpec3))|
                (F.col('mtd_16_19') < F.first('mtd_16_19',True).over(windowSpec2)),
                F.lag('mtd_16_19').over(windowSpec2)).otherwise(F.col('mtd_16_19'))).alias('mtd_16_19')))


column_etl_8 = (column_etl_7.select(*[x for x in column_etl_7.columns if x not in ('mtd_20_23')], 
                (F.when((F.col('mtd_20_23') > F.first('mtd_20_23',True).over(windowSpec3))|
                (F.col('mtd_20_23') < F.first('mtd_20_23',True).over(windowSpec2)),
                F.lag('mtd_20_23').over(windowSpec2)).otherwise(F.col('mtd_20_23'))).alias('mtd_20_23')))

column_etl_9 = (column_etl_8.select(*[x for x in column_etl_8.columns if x not in ('mtd_24_27')], 
                (F.when((F.col('mtd_24_27') > F.first('mtd_24_27',True).over(windowSpec3))|
                (F.col('mtd_24_27') < F.first('mtd_24_27',True).over(windowSpec2)),
                F.lag('mtd_24_27').over(windowSpec2)).otherwise(F.col('mtd_24_27'))).alias('mtd_24_27')))


column_etl_10 = column_etl_9.orderBy(['macaddress','registerat'])




print('----------------------------------result success----------------------------------')


time.sleep(5)

print('----------------------------------s3 write ready----------------------------------')


# dynamic frame transform
applymapping = DynamicFrame.fromDF(column_etl_10, glueContext , "applymapping")

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