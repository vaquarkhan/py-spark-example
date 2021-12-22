from pyathena.pandas.util import to_sql
from pyathena.async_cursor import AsyncCursor
from pyathena import connect
from pygrok import Grok
import functools
import logging
import random
import time
import pandas as pd
import threading
import boto3
import re

S3_BUCKET_NAME = 's3-an2-bdp-dev-datalake'
TABLE_S3_SAVE_FORMAT = 'TEXTFILE'
TABLE_COMPRESSION_TYPE = 'SNAPPY'
S3_SAVE_PATH_FORMAT = "s3://{}/data/{}/{}/"

athena = connect(s3_staging_dir="s3://s3-an2-bdp-dev-temp/athena/",
                 region_name="ap-northeast-2")

s3 = boto3.resource('s3')


def make_logger(name=None):
    logger = logging.getLogger(name)

    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter("[%(asctime)-10s] (줄 번호: %(lineno)d) %(name)s:%(levelname)s - %(message)s")

    console = logging.StreamHandler()
    file_handler = logging.FileHandler(filename="spark_to_athena.log")

    console.setLevel(logging.INFO)
    file_handler.setLevel(logging.DEBUG)

    console.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    logger.addHandler(console)
    logger.addHandler(file_handler)

    return logger


logger = make_logger("hive_to_athena_convert_query")


def retry(total_try_cnt=5, sleep_in_sec=5):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for cnt in range(total_try_cnt):
                logger.info(f"trying {func.__name__}() [{cnt + 1}/{total_try_cnt}]")

                try:
                    result = func(*args, **kwargs)
                    logger.info(f"in retry(), {func.__name__}() returned '{result}'")

                    if result is not None:
                        return result
                except Exception as e:
                    logger.info(e)
                    logger.info(f"in retry(), {func.__name__}() raised {e}")
                    pass

                time.sleep(sleep_in_sec)
            logger.error(f"{func.__name__} finally has been failed")
            exit(1)
        return wrapper

    return decorator


def grok_query_extract(query, pattern):
    result = Grok(pattern).match(query)
    logger.debug(f"Grok Result : {result}")
    return result


@retry(total_try_cnt=3, sleep_in_sec=1)
def run_athena_query(query, pandas=False, async_flag=False):
    logger.debug(f"Run Athena Query : {query}")
    if async_flag:
        query_id, future = athena.cursor(AsyncCursor).execute(query)
        result_set = future.result()
        return result_set
    else:
        if pandas:
            return pd.read_sql(query, athena)
        else:
            return athena.cursor().execute(query)


def list_chunk(lst, n):
    return [lst[i:i + n] for i in range(0, len(lst), n)]


def remove_athena_table_s3_data(database_name, table_name, partition_column_name=None, partition_list=None):
    logger.info("Run Function : remove_athena_table_s3_data")
    logger.debug(f"Database Name : {database_name}")
    logger.debug(f"Table Name : {table_name}")
    logger.debug(f"Partition Column Name : {partition_column_name}")
    logger.debug(f"Partition List : {partition_list}")
    bucket = s3.Bucket(S3_BUCKET_NAME)
    if partition_column_name and partition_list:
        for partition_column_index in partition_list:
            bucket.object_versions.filter(
                Prefix=f"{S3_SAVE_PATH_FORMAT.split('/{}/')[1]}/{database_name.replace('_dev','')}/{table_name}/{partition_column_name}={partition_column_index}/").delete()
    else:
        bucket.object_versions.filter(
            Prefix=f"{S3_SAVE_PATH_FORMAT.split('/{}/')[1]}/{database_name.replace('_dev','')}/{table_name}/").delete()
    return 0


def run_drop_table(query):
    logger.info("Run Function : run_drop_table")
    pattern = "drop\s+table\s+(if\s+exists\s+)?%{WORD:database_name}.%{WORD:table_name}(\s|\;)?"
    query_lower = query.lower()
    table_info = grok_query_extract(query_lower, pattern)

    remove_athena_table_s3_data(database_name=table_info['database_name'].replace('_dev', ''),
                                table_name=table_info['table_name'])

    replace_query = f"DROP TABLE IF EXISTS {table_info['database_name']}.{table_info['table_name']}"

    run_athena_query(replace_query)
    return 0


def get_partition_list(query):
    logger.info("Run Function : get_partition_list")
    query_lower = query.lower()
    partition_column_name = grok_query_extract(query_lower, "partition\s+\(%{WORD:partition_column}\)")['partition_column']
    index = query_lower.find('select')
    if index > 0:
        query = query[index:]
    query = f"SELECT DISTINCT {partition_column_name} FROM ({query})"
    result = run_athena_query(query, pandas=True)

    return list_chunk(list_chunk(list(result[partition_column_name].sort_values()), 99), 10)


def function_insert_overwrite_multi_loop(query, partition_column_list):
    logger.info("Run Function : function_insert_overwrite_multi_loop")
    pattern = "insert\s+overwrite\s+table\s+%{WORD:database_name}.%{WORD:table_name}(\s|\;)?partition\s+\(%{WORD:partition_column}\)?"
    query_lower = query.lower()
    table_info = grok_query_extract(query_lower, pattern)
    remove_athena_table_s3_data(table_info['database_name'].replace('_dev', ''), table_info['table_name'], table_info['partition_column'],
                                partition_column_list)
    thread_query = f"INSERT INTO {table_info['database_name']}.{table_info['table_name']}\nSELECT * FROM (\n{query[query_lower.find('select'):]})\nWHERE {table_info['partition_column']}>='{partition_column_list[0]}' AND {table_info['partition_column']}<='{partition_column_list[-1]}' "

    run_athena_query(thread_query, async_flag=False)
    return 0


def run_insert_overwrite_multi_loop(query):
    logger.info("Run Function : run_insert_overwrite_multi_loop")
    for loop_list in get_partition_list(query):
        threads = []

        for partition_column_list in loop_list:
            thread = threading.Thread(target=function_insert_overwrite_multi_loop, args=(query, partition_column_list))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()
    return 0


def run_insert(query):
    logger.info("Run Function : run_insert")
    pattern = "insert\s+(overwrite|into)?\s+(table\s+)?+%{WORD:database_name}.%{WORD:table_name}(\s|\;)?"
    query_lower = query.lower()
    table_info = grok_query_extract(query_lower, pattern)
    if re.compile("insert\s+overwrite\s+(table\s+)?\w+\.\w+\s+partition\s+\(\w+\)", re.IGNORECASE).search(query):
        run_insert_overwrite_multi_loop(query)
    elif re.compile("insert\s+overwrite\s+(table\s+)?\w+\.\w+", re.IGNORECASE).search(query):
        remove_athena_table_s3_data(table_info['database_name'], table_info['table_name'])
        replace_query = f"INSERT INTO {table_info['database_name']}.{table_info['table_name']}\n{query[re.compile('(select|values)', re.IGNORECASE).search(query).start():]}"
        run_athena_query(replace_query)
    else:
        replace_query = f"INSERT INTO {table_info['database_name']}.{table_info['table_name']}\n{query[re.compile('(select|values)', re.IGNORECASE).search(query).start():]}"
        run_athena_query(replace_query)
    return 0


def replace_create_hive_query_to_athena_query(query):
    logger.info("Run Function : replace_create_hive_query_to_athena_query")
    pattern = "create\s+table\s+(if\s+not\s+exists\s+)?%{WORD:database_name}.%{WORD:table_name}(\s|\;)?"
    query_lower = query.lower()
    table_info = grok_query_extract(query_lower, pattern)
    option = [f"STORED AS {TABLE_S3_SAVE_FORMAT}",
              "LOCATION '{}'".format(
                  S3_SAVE_PATH_FORMAT.format(S3_BUCKET_NAME, table_info['database_name'].replace('_dev', ''), table_info['table_name'])),
              f"tblproperties('orc_compression' = '{TABLE_COMPRESSION_TYPE}')"
              ]

    query = re.compile('tblproperties.*\)(\n|\s)', re.IGNORECASE).sub('\n', query)
    query = re.compile('stored\s+as\s+\w+(\n)?', re.IGNORECASE).sub('\n', query)
    query = "{}\n{}".format((re.compile('CREATE TABLE', re.IGNORECASE).sub('CREATE EXTERNAL TABLE', query)).strip(), "\n".join(option))

    return query


def run_ctas(query):
    logger.info("Run Function : run_ctas")
    pattern = "create\s+table\s+(if\s+not\s+exists\s+)?%{WORD:database_name}.%{WORD:table_name}(\s|\;)?"
    query_lower = query.lower()
    table_info = grok_query_extract(query_lower, pattern)
    option = ["format = '{}'".format(TABLE_S3_SAVE_FORMAT),
              "external_location = '{}'".format(
                  S3_SAVE_PATH_FORMAT.format(S3_BUCKET_NAME, table_info['database_name'].replace('_dev', ''), table_info['table_name'])),
              f"orc_compression = '{TABLE_COMPRESSION_TYPE}'"
              ]
    replace_query = "CREATE TABLE IF NOT EXISTS {}.{} WITH ({}) {}".format(table_info['database_name'], table_info['table_name'], ','.join(option), query[re.compile('((as(\s+|\n+)select)|(as(\s+|\n+)with))', re.IGNORECASE).search(query).start():])

    run_athena_query(replace_query)
    return 0


def run_create_table(query):
    logger.info("Run Function : run_create_table")
    query = re.compile('row format .*\n', re.IGNORECASE).sub('', query)

    if query.lower().find('select') < 0:
        run_athena_query(replace_create_hive_query_to_athena_query(query))
    else:
        run_ctas(query)
    return 0


def run_load_data(query):
    logger.info("Run Function : run_load_data")
    pattern = "load\s+data\s+(local\s+)?inpath\s+\'%{DATA:load_file_path}\'\s+(overwrite\s+)?into\s+table\s+%{WORD:database_name}.%{WORD:table_name}(\s|\;)?" + \
              "(partition\s+\((|\s+)?%{WORD:partition_column_name}(|\s+)?=(|\s+)?\'%{WORD:partition_list}\'(|\s+)?\))?"
    query_lower = query.lower()
    table_info = grok_query_extract(query_lower, pattern)

    if query_lower.find('overwrite') > 0:
        remove_athena_table_s3_data(table_info['database_name'],
                                    table_info['table_name'],
                                    table_info['partition_column_name'],
                                    table_info['partition_list'])

    load_df = pd.read_csv(table_info['load_file_path'])
    to_sql(load_df, table_info['table_name'], athena,
           S3_SAVE_PATH_FORMAT.format(S3_BUCKET_NAME, table_info['database_name'].replace('_dev', ''), table_info['table_name']),
           schema=table_info['database_name'], if_exists="replace")
    return 0


def run_select(query):
    logger.info("Run Function : run_select")
    return run_athena_query(query, pandas=True)


def run_truncate(query):
    logger.info("Run Function : run_truncate")
    pattern = "truncate\s+(table\s+)?%{WORD:database_name}.%{WORD:table_name}(\s|\;)?"
    query_lower = query.lower()
    table_info = grok_query_extract(query_lower, pattern)
    remove_athena_table_s3_data(table_info['database_name'].replace('_dev', ''),
                                table_info['table_name'])
    return 0


def run_alter_table_add_partition(database_name, table_name=None):
    bucket = s3.Bucket(S3_BUCKET_NAME)
    if table_name:
        loop_list = bucket.object_versions.filter(Prefix=f"{S3_SAVE_PATH_FORMAT.split('/{}/')[1]}/{database_name}/{table_name}/")
    else:
        loop_list = bucket.object_versions.filter(Prefix=f"{S3_SAVE_PATH_FORMAT.split('/{}/')[1]}/{database_name}/")
    partition_list = {}
    for s3_table_data in loop_list:
        partition_info = s3_table_data.object_key.split('/')[3]
        table_name = s3_table_data.object_key.split('/')[2]
        if re.compile('\w+\=\w+').match(partition_info):
            if not partition_list.get(table_name):
                partition_list[table_name] = []
            partition_condition = f"partition ({partition_info.split('=')[0]} = '{partition_info.split('=')[1]}')"
            if not partition_condition in partition_list[table_name]:
                partition_list[table_name].append(partition_condition)
    for table_name in partition_list.keys():
        query = "alter table {}.{} add if not exists {}".format(database_name, table_name, '\n'.join(list(set(partition_list[table_name]))))
        print(query)
        run_athena_query(query)


def run_etc(query):
    logger.info("Run Function : run_etc")
    if re.compile("(analyze|alter)", re.IGNORECASE).match(query):
        return
    else:
        run_athena_query(query)

    return 0


def temp_func_for_dev(query):
    query = re.compile('dm_r', re.IGNORECASE).sub('DM_R_DEV', query)
    query = re.compile('temp_r', re.IGNORECASE).sub('TEMP_R_DEV', query)
    query = re.compile('source_v3', re.IGNORECASE).sub('SOURCE_V3_DEV', query)
    # query = re.compile('source', re.IGNORECASE).sub('source_dev', query)
    query = re.compile('result_r', re.IGNORECASE).sub('RESULT_R_DEV', query)
    return query


def hive_to_ahtena_convert_query(query):
    logger.info("Run Query : {}".format(query))
    # query = re.compile('--.*\n').sub('\n', query).replace('\"', '\'').strip()
    query = re.compile('--.*(|$)').sub('\n', query).replace('\"', '\'').strip()
    query = temp_func_for_dev(query)
    try:
        if re.compile("create", re.IGNORECASE).match(query):
            return run_create_table(query)
        elif re.compile("insert", re.IGNORECASE).match(query):
            return run_insert(query)
        elif re.compile("drop", re.IGNORECASE).match(query):
            return run_drop_table(query)
        elif re.compile("load", re.IGNORECASE).match(query):
            return run_load_data(query)
        elif re.compile("truncate", re.IGNORECASE).match(query):
            return run_truncate(query)
        elif re.compile("select", re.IGNORECASE).match(query):
            return run_select(query)
        else:
            return run_etc(query)
        logger.info("Query Complete : {}".format(query))

    except Exception as e:
        logger.error(e)
        logger.error("Query Failed : {}".format(query))



if __name__=='__main__':
    # hive_to_ahtena_convert_query("drop table TEMP_R.AIQA_ADJUSTED_Y_3Y_ATHENA")
    df = hive_to_ahtena_convert_query("""CREATE TABLE TEMP_R.AIQA_ADJUSTED_Y_3Y_ATHENA_2 AS 
SELECT
    BETF_JG_NO
	 , JG_RSK_ID 
     , CASE WHEN NER = '정상인수' THEN '2001' 
			WHEN NER = '표준미달체' THEN '2002'
			WHEN NER = '거절' THEN '2003'
	   ELSE '2004' END AS ADJUSTED_Y
FROM DM_R.AIQA_DM_AIUW_BFWR_RSLT
WHERE NER_TAG = 'RESULT' AND BASE_YM>='201712'""")
    print(df)
