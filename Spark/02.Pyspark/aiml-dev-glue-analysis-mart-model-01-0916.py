import sys
import time
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from awsglue.gluetypes import *
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import HiveContext
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
import subprocess

subprocess.call([sys.executable, '-m', 'pip', 'install', 'pyarrow==1.0.1'])
subprocess.call([sys.executable, '-m', 'pip', 'install', 'pandas==1.1.5'])

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

# spark.sql.autoBroadcastJoinThreshold = -1
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# # Partition Key Column Name
partition_name = ['year','month','day']

today = datetime.today().strftime("%Y-%m-%d")   
year = today.split('-')[0]
month = str(int(today.split('-')[1]) - 1).zfill(2)

if str(int(month) - 1).zfill(2) == '00':
    year = int(year) - 1
    month = '12'


push_down_condition = f"(year == '{year}' and month == '{month}')"

# S3_location = 's3://aiml-dev-anomaly-detection/'
# S3_location = 's3://aiml-ml-test/model_test_mac_list/'
# S3_location = 's3://aiml-ml-test/model_test_mac_list_writemaclist/'
S3_location = 's3://aiml-ml-test/model-1-mart/'



cols_to_update = ['errorlabel']


def error_label(temp):
  import datetime
  import pandas as pd
  
  temp = temp.sort_values('start')
  temp['rank'] = temp.groupby(['macaddress']).cumcount()
  temp = temp.reset_index(drop=True)
  
  ErrorDate = temp[(temp['errorlabel'] == 1) | (temp['errorlabel']==2)][list(temp.columns)]
  ErrorDate['start'] = pd.to_datetime(ErrorDate['start'])
  
  for i in range(len(ErrorDate)):
    start = ErrorDate.start[ErrorDate.index[i]] - datetime.timedelta(days=1)
    while len(temp[temp['start']==str(start)].index.values) == 0 and str(start) != str(ErrorDate.start[ErrorDate.index[i]]):
      start = start + datetime.timedelta(minutes=10)
    idx = int(min(temp[temp['start']==str(start)].index.values))
    # print(idx)
    # print(int(ErrorDate['rank'].iloc[i]))
    for j in range(idx, int(ErrorDate['rank'].iloc[i])):
    #   if temp.iloc[j]['errorlabel'] == 0:
      if (temp.iloc[j]['errorlabel'] == 0) | (temp.iloc[j]['errorlabel'] == 3):
        temp['errorlabel'][j] = ErrorDate.errorlabel[ErrorDate.index[i]]
      else : 
          pass
  return temp.drop(columns=['rank'])
  
# def version(temp):
#   import pandas as pd
#   import copy
#   temp = temp.sort_values('start', ascending=False)
#   temp = temp.reset_index(drop=True)
  
  
#   # save latest values in dictionary
#   l_values = {
#     'roomcontrollerswversion': copy.deepcopy(temp['roomcontrollerswversion'].iloc[0]),
#     'boilercontrollerswversion': copy.deepcopy(temp['boilercontrollerswversion'].iloc[0]),
#     'boilercontrollerswsubversion': copy.deepcopy(temp['boilercontrollerswsubversion'].iloc[0])
#              }
  
#   # exception
#   # fill the latest value to null value
#   if pd.isna(temp['roomcontrollerswversion'].iloc[0]):
#       l_values['roomcontrollerswversion'] = temp.iloc[temp.index[pd.isna(temp['roomcontrollerswversion'])==False].tolist()[0]]['roomcontrollerswversion']  # get value
 
#   if pd.isna(temp['boilercontrollerswversion'].iloc[0]):
#       l_values['boilercontrollerswversion'] = temp.iloc[temp.index[pd.isna(temp['boilercontrollerswversion'])==False].tolist()[0]]['boilercontrollerswversion']  # get value
 
#   if pd.isna(temp['boilercontrollerswsubversion'].iloc[0]):
#       l_values['boilercontrollerswsubversion'] = temp.iloc[temp.index[pd.isna(temp['boilercontrollerswsubversion'])==False].tolist()[0]]['boilercontrollerswsubversion']  # get value
  
#   for i in range(len(temp)):  # reverse for loop. cause indexing propblem
#     if pd.isna(temp.iloc[i]['roomcontrollerswversion']):  # is null
#         temp['roomcontrollerswversion'].iloc[i] = l_values['roomcontrollerswversion']  # is null then fill l_values
#     else:
#         l_values['roomcontrollerswversion'] = copy.deepcopy(temp['roomcontrollerswversion'].iloc[i])
 
#     if pd.isna(temp.iloc[i]['boilercontrollerswversion']):  # is null
#         temp['boilercontrollerswversion'].iloc[i] = l_values['boilercontrollerswversion']  # is null then fill l_values
#     else:
#         l_values['boilercontrollerswversion'] = copy.deepcopy(temp['boilercontrollerswversion'].iloc[i])
      
#     if pd.isna(temp.iloc[i]['boilercontrollerswsubversion']):  # is null
#         temp['boilercontrollerswsubversion'].iloc[i] = l_values['boilercontrollerswsubversion']  # is null then fill l_values
#     else:
#         l_values['boilercontrollerswsubversion'] = copy.deepcopy(temp['boilercontrollerswsubversion'].iloc[i])
 
 
#   return temp.sort_values(["macaddress","start"])
  
def version(temp):
  import pandas as pd
  import copy
  temp = temp.sort_values('start', ascending=False)
  temp = temp.reset_index(drop=True)
  
  
  # save latest values in dictionary
  l_values = {
    'roomcontrollerswversion': copy.deepcopy(temp['roomcontrollerswversion'].iloc[0]),
    'boilercontrollerswversion': copy.deepcopy(temp['boilercontrollerswversion'].iloc[0]),
    'boilercontrollerswsubversion': copy.deepcopy(temp['boilercontrollerswsubversion'].iloc[0])
             }
  
  # exception
  # fill the latest value to null value
  if pd.isna(temp['roomcontrollerswversion'].iloc[0]):
    l_values['roomcontrollerswversion'] = temp.iloc[temp.index[pd.isna(temp['roomcontrollerswversion'])==False].tolist()[0]]['roomcontrollerswversion']  # get value
 
  if pd.isna(temp['boilercontrollerswversion'].iloc[0]):
    l_values['boilercontrollerswversion'] = temp.iloc[temp.index[pd.isna(temp['boilercontrollerswversion'])==False].tolist()[0]]['boilercontrollerswversion']  # get value
 
  if pd.isna(temp['boilercontrollerswsubversion'].iloc[0]):
    l_values['boilercontrollerswsubversion'] = temp.iloc[temp.index[pd.isna(temp['boilercontrollerswsubversion'])==False].tolist()[0]]['boilercontrollerswsubversion']  # get value
  
  for i in range(len(temp)):  # reverse for loop. cause indexing propblem
 
    if pd.isna(temp.iloc[i]['roomcontrollerswversion']):  # is null
      temp['roomcontrollerswversion'].iloc[i] = l_values['roomcontrollerswversion']  # is null then fill l_values
    else:
      l_values['roomcontrollerswversion'] = copy.deepcopy(temp['roomcontrollerswversion'].iloc[i])
 
    if pd.isna(temp.iloc[i]['boilercontrollerswversion']):  # is null
      temp['boilercontrollerswversion'].iloc[i] = l_values['boilercontrollerswversion']  # is null then fill l_values
    else:
      l_values['boilercontrollerswversion'] = copy.deepcopy(temp['boilercontrollerswversion'].iloc[i])
      
    if pd.isna(temp.iloc[i]['boilercontrollerswsubversion']):  # is null
      temp['boilercontrollerswsubversion'].iloc[i] = l_values['boilercontrollerswsubversion']  # is null then fill l_values
    else:
      l_values['boilercontrollerswsubversion'] = copy.deepcopy(temp['boilercontrollerswsubversion'].iloc[i])
 
 
  return temp.sort_values(["macaddress","start"])

# # --------------------------------------------
# # -- START
# # --------------------------------------------

print('----------------------------------start---------------------------------')
# Read from Catalog 

# read_alive002 = glueContext.create_dynamic_frame.from_catalog(database = 'kd_etl', table_name = 'aiml_dev_alive_002_etl_result', transformation_ctx = "read_alive002" , push_down_predicate = push_down_condition)
# read_alive003 = glueContext.create_dynamic_frame.from_catalog(database = 'kd_etl', table_name = 'aiml_dev_alive_003_etl_result', transformation_ctx = "read_alive003" , push_down_predicate = push_down_condition)

# read_connection = glueContext.create_dynamic_frame.from_catalog(database = 'kd_etl', table_name = 'aiml_dev_event_connection_2nd_etl', transformation_ctx = "read_connection", push_down_predicate = push_down_condition)

# read_ehrd002 = glueContext.create_dynamic_frame.from_catalog(database = 'kd_etl', table_name = 'aiml_dev_event_ehrd_002_etl', transformation_ctx = "read_ehrd002" , push_down_predicate = push_down_condition)
# read_ehrd003 = glueContext.create_dynamic_frame.from_catalog(database = 'kd_etl', table_name = 'aiml_dev_event_ehrd_003_etl', transformation_ctx = "read_ehrd003" , push_down_predicate = push_down_condition)

# read_ehrd002 = glueContext.create_dynamic_frame.from_catalog(database = 'kd_etl', table_name = 'aiml_dev_event_ehrd_002_etl', transformation_ctx = "read_ehrd002")
# read_ehrd003 = glueContext.create_dynamic_frame.from_catalog(database = 'kd_etl', table_name = 'aiml_dev_event_ehrd_003_etl', transformation_ctx = "read_ehrd003")


# read_did002 = glueContext.create_dynamic_frame.from_catalog(database = 'kd_etl', table_name = 'aiml_dev_response_did_002_etl', transformation_ctx = "read_did002" , push_down_predicate = push_down_condition)
# read_did003 = glueContext.create_dynamic_frame.from_catalog(database = 'kd_etl', table_name = 'aiml_dev_response_did_003_etl', transformation_ctx = "read_did003" , push_down_predicate = push_down_condition)

# read_status002 = glueContext.create_dynamic_frame.from_catalog(database = 'kd_etl', table_name = 'aiml_dev_response_status_002_etl', transformation_ctx = "read_status002" , push_down_predicate = push_down_condition)
# read_status003 = glueContext.create_dynamic_frame.from_catalog(database = 'kd_etl', table_name = 'aiml_dev_response_status_003_etl', transformation_ctx = "read_status003" , push_down_predicate = push_down_condition)




read_alive002 = glueContext.create_dynamic_frame.from_catalog(database = 'kd_etl', table_name = 'aiml_dev_alive_002_etl_result', transformation_ctx = "read_alive002")
read_alive003 = glueContext.create_dynamic_frame.from_catalog(database = 'kd_etl', table_name = 'aiml_dev_alive_003_etl_result', transformation_ctx = "read_alive003")

read_connection = glueContext.create_dynamic_frame.from_catalog(database = 'kd_etl', table_name = 'aiml_dev_event_connection_2nd_etl', transformation_ctx = "read_connection")

read_ehrd002 = glueContext.create_dynamic_frame.from_catalog(database = 'kd_etl', table_name = 'aiml_dev_event_ehrd_002_etl', transformation_ctx = "read_ehrd002")
read_ehrd003 = glueContext.create_dynamic_frame.from_catalog(database = 'kd_etl', table_name = 'aiml_dev_event_ehrd_003_etl', transformation_ctx = "read_ehrd003")

read_did002 = glueContext.create_dynamic_frame.from_catalog(database = 'kd_etl', table_name = 'aiml_dev_response_did_002_etl', transformation_ctx = "read_did002")
read_did003 = glueContext.create_dynamic_frame.from_catalog(database = 'kd_etl', table_name = 'aiml_dev_response_did_003_etl', transformation_ctx = "read_did003")

read_status002 = glueContext.create_dynamic_frame.from_catalog(database = 'kd_etl', table_name = 'aiml_dev_response_status_002_etl', transformation_ctx = "read_status002")
read_status003 = glueContext.create_dynamic_frame.from_catalog(database = 'kd_etl', table_name = 'aiml_dev_response_status_003_etl', transformation_ctx = "read_status003")




print('----------------------------------success read catalog----------------------------------')

# Transform to SparkDF

# join 시 null 값에 존재하는건 0으로 치환 , 
ehrd002_df_raw = read_ehrd002.toDF()
ehrd003_df_raw = read_ehrd003.toDF()


ehrd002_df = (ehrd002_df_raw.withColumn('errorlabel',
                    (F.when((F.col('errorcode_low') == 3) & (F.col('errorcode_high') == 0) ,1))
                      .when((F.col('errorcode_low') == 12) & (F.col('errorcode_high') == 0) ,2).otherwise(F.lit(3))))



ehrd003_df = (ehrd003_df_raw.withColumn('errorlabel',
                    (F.when((F.col('errorcode_low') == 3) & (F.col('errorcode_high') == 0) ,1))
                      .when((F.col('errorcode_low') == 12) & (F.col('errorcode_high') == 0) ,2).otherwise(F.lit(3))))
                      

                      
# ehrd002_df_mac_address_list = ehrd002_df.where('errorlabel == 1 | errorlabel == 2').select('macaddress')
# ehrd002_df_mac_address_list.write.format('csv').mode('overwrite').option('header','true').save('s3://aiml-dev-weather/macaddress_list/macaddress_002/')

# ehrd003_df_mac_address_list = ehrd003_df.where('errorlabel == 1 | errorlabel == 2').select('macaddress')
# ehrd003_df_mac_address_list.write.format('csv').mode('overwrite').option('header','true').save('s3://aiml-dev-weather/macaddress_list/macaddress_003/')


ehrd002_df_mac_address_list = ehrd002_df.where('errorlabel == 1 or errorlabel == 2').select('macaddress')
ehrd002_df_mac_address_list = ehrd002_df_mac_address_list.dropDuplicates()


ehrd002_write = ehrd002_df_mac_address_list.write.format('csv').mode('overwrite').option('header','true').save('s3://aiml-dev-weather/macaddress_list/macaddress_002_test/')


ehrd003_df_mac_address_list = ehrd003_df.where('errorlabel == 1 or errorlabel == 2').select('macaddress')
ehrd003_df_mac_address_list = ehrd003_df_mac_address_list.dropDuplicates()


ehrd003_write = ehrd003_df_mac_address_list.write.format('csv').mode('overwrite').option('header','true').save('s3://aiml-dev-weather/macaddress_list/macaddress_003_test/')




time.sleep(5)
print('--------------------------write macaddress list -----------------------------------------')

alive002_df = read_alive002.toDF()
alive003_df = read_alive003.toDF()

# alive002_standard_deviation = spark.read.format('csv').option('header','True').load('s3://aiml-dev-weather/macaddress_002_list_20210904132125.csv')
# alive003_standard_deviation = spark.read.format('csv').option('header','True').load('s3://aiml-dev-weather/macaddress_003_list_20210904132125.csv')

alive002_standard_deviation = spark.read.format('csv').option('header','True').load('s3://aiml-dev-weather/macaddress_list/macaddress_002_test/')
alive003_standard_deviation = spark.read.format('csv').option('header','True').load('s3://aiml-dev-weather/macaddress_list/macaddress_003_test/')


macaddress_list_alive002 = []
for row in alive002_standard_deviation.select('macaddress').collect():
    macaddress_list_alive002.append(row[0])

macaddress_list_alive003 = []
for row in alive003_standard_deviation.select('macaddress').collect():
    macaddress_list_alive003.append(row[0])

alive002_df = alive002_df.where(F.col("macaddress").isin(macaddress_list_alive002))
alive003_df = alive003_df.where(F.col("macaddress").isin(macaddress_list_alive003))

print('----------------------------------success extract macaddress----------------------------------')

status002_df = read_status002.toDF()
status003_df = read_status003.toDF()

# # join 시 null 값에 존재하는건 0으로 치환 , 
# ehrd002_df_raw = read_ehrd002.toDF()
# ehrd003_df_raw = read_ehrd003.toDF()

did002_df = read_did002.toDF()
did003_df = read_did003.toDF()

connection_df = read_connection.toDF()

# ehrd002_df = (ehrd002_df_raw.withColumn('errorlabel',
#                     (F.when((F.col('errorcode_low') == 3) & (F.col('errorcode_high') == 0) ,1))
#                       .when((F.col('errorcode_low') == 12) & (F.col('errorcode_high') == 0) ,2).otherwise(F.lit(3))))

# ehrd003_df = (ehrd003_df_raw.withColumn('errorlabel',
#                     (F.when((F.col('errorcode_low') == 3) & (F.col('errorcode_high') == 0) ,1))
#                       .when((F.col('errorcode_low') == 12) & (F.col('errorcode_high') == 0) ,2).otherwise(F.lit(3))))
                      



join_temp_002 = alive002_df.join(status002_df,(alive002_df["macaddress"] == status002_df["macaddress"])&(alive002_df["start"] == status002_df["start"]),'left').drop(status002_df["macaddress"]).drop(status002_df['additionalvalue']).drop(status002_df['start']).drop(status002_df['year']).drop(status002_df['end']).drop(status002_df['day']).drop(status002_df['month']) \
            .join(ehrd002_df,(alive002_df["macaddress"] == ehrd002_df["macaddress"])&(alive002_df["start"] == ehrd002_df["start"]),'left').drop(ehrd002_df["macaddress"]).drop(ehrd002_df['start']).drop(ehrd002_df['additionalvalue']).drop(ehrd002_df['year']).drop(ehrd002_df['end']).drop(ehrd002_df['day']).drop(ehrd002_df['month']) \
            .join(did002_df,(alive002_df["macaddress"]   == did002_df["macaddress"])&(alive002_df["start"] == did002_df["start"]),'left').drop(did002_df["macaddress"]).drop(did002_df['start']).drop(did002_df['additionalvalue']).drop(did002_df['year']).drop(did002_df['end']).drop(did002_df['day']).drop(did002_df['month'])



join_temp_002 = join_temp_002.withColumn('type', F.lit(2).cast('integer'))
  

join_temp_003 = alive003_df.join(status003_df,(alive003_df["macaddress"] == status003_df["macaddress"])&(alive003_df["start"] == status003_df["start"]),'left').drop(status003_df["macaddress"]).drop(status003_df['additionalvalue']).drop(status003_df['start']).drop(status003_df['year']).drop(status003_df['end']).drop(status003_df['day']).drop(status003_df['month']) \
            .join(ehrd003_df,(alive003_df["macaddress"] == ehrd003_df["macaddress"])&(alive003_df["start"] == ehrd003_df["start"]),'left').drop(ehrd003_df["macaddress"]).drop(ehrd003_df['start']).drop(ehrd003_df['additionalvalue']).drop(ehrd003_df['year']).drop(ehrd003_df['end']).drop(ehrd003_df['day']).drop(ehrd003_df['month']) \
            .join(did003_df,(alive003_df["macaddress"]   == did003_df["macaddress"])&(alive003_df["start"] == did003_df["start"]),'left').drop(did003_df["macaddress"]).drop(did003_df['start']).drop(did003_df['additionalvalue']).drop(did003_df['year']).drop(did003_df['end']).drop(did003_df['day']).drop(did003_df['month'])            
          

join_temp_003 = join_temp_003.withColumn('type', F.lit(3).cast('integer'))

print('----------------------------------talbe join success----------------------------------')


unionDF = join_temp_002.unionByName(join_temp_003)
unionDF_connection_join = unionDF.join(connection_df,(unionDF["macaddress"] == connection_df["macaddress"])&(unionDF["start"] == connection_df["start"]),'left').drop(connection_df["macaddress"]).drop(connection_df['start']).drop(connection_df['year']).drop(connection_df['end']).drop(connection_df['day']).drop(connection_df['month'])
unionDF = unionDF_connection_join.na.fill(value=0,subset=['errorlabel','errorcode_low','errorcode_high','servicecode_low','servicecode_high','errorlevel','swversion','releaseyear_low','releaseyear_high','releasedate_low','releasedate_high','errortime_low','errortime_high']).na.drop(subset=['operationmode','insidetemperature','hotwatertemperaturesetting','insidetemperaturesetting','operationbusy'])
print('----------------------------------table union , na fill zero----------------------------------')

print('----------------------------------table union , na fill zero----------------------------------')


weather_df = spark.read.format('csv').option('header','true').load('s3://aiml-dev-weather/weather_v2.csv/')
join_anlDF = unionDF.join(weather_df,(unionDF['start'] == weather_df['datetime']),'left').drop(weather_df['datetime'])

print('----------------------------------join weather column----------------------------------')


pdf = join_anlDF

error_label_udf = F.pandas_udf(error_label, pdf.schema, functionType=F.PandasUDFType.GROUPED_MAP)
pdf_error_2 = pdf.groupBy('macaddress').apply(error_label_udf).orderBy(['macaddress','start'])

version_label_udf = F.pandas_udf(version, pdf_error_2.schema, functionType=F.PandasUDFType.GROUPED_MAP)
version_labelDF = pdf_error_2.groupBy('macaddress').apply(version_label_udf).orderBy(['macaddress','start'])




version_labelDF.printSchema()
# resultDF = version_labelDF.selectExpr('*','cast(year as int) year','cast(month as int) month','cast(day as int) day','cast(temperature as double) temperature')
resultDF = version_labelDF

# resultDF = version_labelDF.selectExpr('*','cast(temperature as double) temperature')

print('----------------------------------add error label 2----------------------------------')


print('----------------------------------result success----------------------------------')



time.sleep(5)

print('----------------------------------s3 write ready----------------------------------')



resultDF.write.format('csv').mode('overwrite').option('header','true').save(S3_location)

# dynamic frame transform
# applymapping = DynamicFrame.fromDF(resultDF, glueContext , "applymapping")



# write parquet
# datasink = glueContext.write_dynamic_frame_from_options(
#               frame=applymapping,
#               connection_type="s3",
#               connection_options={
#                   "path": S3_location,
#                   "partitionKeys": partition_name
#               },
#               format="glueparquet",
#               format_options = {"compression": "snappy"},
#               transformation_ctx ="datasink")


# csv write

# datasink = glueContext.write_dynamic_frame_from_options(
#               frame=applymapping,
#               connection_type="s3",
#               connection_options={
#                   "path": S3_location,
#                   "partitionKeys": partition_name
#               },
#               format="csv",
#               format_options={
#                     "quoteChar": -1, 
#                     "separator": ",",
#                     "multiLine" : True,
#                     "withHeader" : True
#               }, 
#               transformation_ctx ="datasink")
              
print('----------------------------------S3 Write success----------------------------------')
time.sleep(5)
print('----------------------------------Job EXIT----------------------------------')
job.commit()