
import sys
import boto3
import json
from awsglue.gluetypes import *
from awsglue.transforms import *
import pyspark.sql.functions as F
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job


## --------------------------------------------
## -- @params: [JOB_NAME]
## --------------------------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)



## --------------------------------------------
## -- secret 매니저를 이용하여 keystore 사용
## --------------------------------------------
client = boto3.client("secretsmanager", region_name="ap-northeast-2")

secret_name = ""

get_secret_value_response = client.get_secret_value(
        SecretId=secret_name
)
secret = get_secret_value_response['SecretString']
secret = json.loads(secret)



## --------------------------------------------
## -- 전역변수
## --------------------------------------------

src_db_schema = ''
src_table_name = ''
partitionsCount = 72
fetchsize = 5000
partition_column = ''
partition_column_toglue = ''
logger = glueContext.get_logger()



## --------------------------------------------
## -- 사용자 정의 SQL
## --------------------------------------------
def getSql():
    use_sql = """
        (
        SELECT
            *
        FROM {db_schema}.{table_name}
        ) tmp
        """.strip('\n').replace('\n', ' ').format(db_schema=src_db_schema, table_name=src_table_name)

    http://logger.info('use_sql : ' + use_sql)
    return use_sql



db_url = 'jdbc:mysql://' + secret['host'] + ':3368/' + secret['dbname'] + '?characterEncoding=utf8&rewriteBatchedStatements=true'
db_username = secret['username']
db_password = secret['password']
bucket_name = ''
sub_folder_name = '/glue-rds-output-buck/'
s3_output_full = ''


## --------------------------------------------
## -- Connecting to the source  (Row Count 계산)
## --------------------------------------------
## Job 실행 전 Row Count를 확인할 수 있는 쿼리 실행

max_sql = "(SELECT MAX(stnd_ymd) FROM `{db_schema}`.{table_name}) as t".strip('\n').replace('\n', ' ').format(db_schema=src_db_schema, table_name=src_table_name)

df_count = glueContext.read.format("jdbc") \
.option("url", db_url) \
.option("dbtable", max_sql) \
.option("user", db_username) \
.option("password", db_password) \
.load()
max_count = df_count.take(1)[0][0]

print('max_count : ' + str(max_count))


min_sql = "(SELECT MIN(stnd_ymd) FROM `{db_schema}`.{table_name}) as t".strip('\n').replace('\n', ' ').format(db_schema=src_db_schema, table_name=src_table_name)

df_count = glueContext.read.format("jdbc") \
.option("url", db_url) \
.option("dbtable", min_sql) \
.option("user", db_username) \
.option("password", db_password) \
.load()
min_count = df_count.take(1)[0][0]

print('min_count : ' + str(min_count))


## --------------------------------------------
## -- Connecting to the source  (병렬 JDBC 사용)
## --------------------------------------------
## partitionColumn : 쿼리 개수를 나누는 기준 컬럼 (date, number, timestamp 타입만)
## lowerBound : 파티션 컬럼의 최소값
## upperBound : 파티션 컬럼의 최대값
## 쿼리 개수 : ( uppperBound - lowerBound ) / numPartitions
## numPartitions : 파티션(분할된 쿼리) 개수
## Worker 가 5개일 경우 :
##   driver = 1개,
##   그 외 excutor = 4개
##   G1X type =  8core
##   numPartition 개수 = (4개 executor) X  ( 8 core ) = 32

df = glueContext.read.format("jdbc") \
.option("url", db_url) \
.option("dbtable", getSql()) \
.option("user", db_username) \
.option("password", db_password) \
.option("fetchSize", fetchsize) \
.option("partitionColumn", partition_column) \
.option("lowerBound", min_count) \
.option("upperBound", max_count) \
.option("numPartitions", partitionsCount) \
.load()

http://logger.info('jdbc dataframe record count : ' + str(df.count()) )
print('jdbc dataframe record count : ' + str(df.count()))

df2 = df.withColumn("stnd_ymd_partition", F.to_date(F.col("stnd_ymd"), "yy-MM"))
df2.printSchema()
print('--------------------')
df2.select('stnd_ymd_partition').show()
## --------------------------------------------
## -- datasource 설정
## --------------------------------------------
datasource0 = DynamicFrame.fromDF(df2, glueContext, "datasource0")

## --------------------------------------------
## -- Defining mapping for the transformation
## --------------------------------------------
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [

], transformation_ctx = "applymapping1")
resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_struct", transformation_ctx = "resolvechoice2")
dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")

## --------------------------------------------
## -- Deleting to destination 초기적재일 경우 테이블 삭제 후 다시 저장
## --------------------------------------------
s3 = boto3.resource('s3')
bucketFull = s3.Bucket(bucket_name)
for obj in bucketFull.objects.filter(Prefix=sub_folder_name[1:] + src_table_name):
    s3.Object(bucketFull.name,obj.key).delete()


## --------------------------------------------
## -- Writing to destination(S3)
## --------------------------------------------
# repartitiondf = dropnullfields3.repartition(1)  # 출력 파일을 1개로 유지할 경우 사용
datasink4 = glueContext.write_dynamic_frame.from_options(
    frame = dropnullfields3,
    connection_type = "s3",
    connection_options = {"path": s3_output_full, "partitionKeys": [partition_column_toglue]},
    format = "glueparquet",
    transformation_ctx = "datasink4"
    )

job.commit()
