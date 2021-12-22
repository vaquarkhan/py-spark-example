import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import pandas as pd
import json
import csv
import re
import urllib.parse
import os
import io
import pickle
import apache_beam.io.fileio
import apache_beam.dataframe.convert



os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:\gcloud_key/mcorpor-mz-test-201104-53172789da37.json"




## 파이프라인 옵션지정 ##
pipeline_options = PipelineOptions(None)
DEST_DIR = "gs://mc_data-bucket/"
options = {
    'project' : 'mcorpor-mz-test-201104',
    'region' : 'us-central1',
    'staging_location' : DEST_DIR + 'staging',
    'temp_location' : DEST_DIR + 'tmp',
    'job_name' : 'mcor-poc-v2',
    'save_main_session' : True,
    'max_num_workers' : 100
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)


def dataframe_convert(line):
    test = json.loads(line)
    df = pd.DataFrame.from_dict([test])
    return df


def parse_test(df):

    domain_lst = ['.com', '.kr', '.net', '.art', '.wiki', '.tv', '.gg', '.org', '.me', '.io', '.top']
    
    # df.renames(columns={'0':'historyId'}, inplace = True)
    
    df['domain'] = ''  # 7행 
    df['keyword'] = '' # 8행
    df['search'] = ''  # 9행

    for i in range(0, len(df)):
        for k in range(0, len(domain_lst)):
            if df.iloc[i,5] == None:  # 3행은 url
                continue

            elif df.iloc[i,5].find(domain_lst[k]) > -1 :
                bunhal = df.iloc[i,5].split(domain_lst[k])
                df.iloc[i,7] = bunhal[0] + domain_lst[k]
                if len(bunhal) > 1 and len(bunhal[1]) > 1 :
                    df.iloc[i,8] = bunhal[1]
  
    keyword = df
    keyword.fillna("None")

# #count = 0

    for i in range(0, len(keyword)):

        url = keyword.iloc[i,8] 
        adj_url = urllib.parse.unquote(url)
        #keyword.iloc[i,9] = adj_url
        if adj_url == "None" :
            continue
        
        # Chrome Style 및 coupang 등 여러 검색엔진 사용
        if adj_url.find('search?query=') > -1 :
            start = adj_url.find('search?query=')
            if adj_url.find('&') > -1:
                end = adj_url.find('&')
                keyword.iloc[i,9] = adj_url[start+len('search?query='):end].replace("+"," ")
            else:
                keyword.iloc[i,9] = adj_url[start+len('search?query='):].replace("+"," ")
        
        elif adj_url.find('search?q') > -1 :
            start = adj_url.find('search?q=')
            if adj_url.find('&') > -1:
                end = adj_url.find('&')
                keyword.iloc[i,9] = adj_url[start+len('search?q='):end].replace("+"," ")
            else:
                keyword.iloc[i,9] = adj_url[start+len('search?q='):].replace("+"," ")
    #        count = count + 1

    # Youtube Style
        elif adj_url.find('search_query') > -1 :
            start = adj_url.find('search_query')
            if adj_url.find('&') > -1:
                end = adj_url.find('&')
                keyword.iloc[i,9] = adj_url[start+len('search_query'):end].replace("+"," ")
            else:
                keyword.iloc[i,9] = adj_url[start+len('search_query'):].replace("+"," ")
    #        count = count + 1   

        elif adj_url.find('query') > -1 :
            start = adj_url.find('query')
            if adj_url.find('&') > -1:
                end = adj_url.find('&')
                keyword.iloc[i,9] = adj_url[start+len('query'):end].replace("+"," ")
            else:
                keyword.iloc[i,9] = adj_url[start+len('query'):].replace("+"," ")



        elif adj_url.find('&q') > -1 :            
            start = adj_url.find('&q')
            if adj_url.find('&oq') > -1 or adj_url.find('&ie') > -1:
                if adj_url.find('&oq') > -1 :
                    end = adj_url.find('&oq')
                    keyword.iloc[i,9] = adj_url[start+len('&oq'):end].replace("+"," ")
                else:
                    end = adj_url.find('&ie')
                    keyword.iloc[i,9] = adj_url[start+len('&oq'):end].replace("+"," ")
        
            else:
                keyword.iloc[i,9] = adj_url[start+len('&oq'):].replace("+"," ")
    
    Json_tran = keyword.to_json(orient = "records", lines =True)

    
    return Json_tran


## 파이프라인 실행 ##
def run(argv=None):
    with beam.Pipeline('DataflowRunner',options=opts) as p:
        from apache_beam.io.gcp.internal.clients import bigquery
        import apache_beam.io.textio
        project_id = 'mcorpor-mz-test-201104'
        dataset_id = 'analytics'
        table_id = 'test_history'
        table_schema = 'historyId:STRING,mac:STRING,browser:STRING,url:STRING,title:STRING,date:STRING,duration:STRING,domain:STRING,keyword:STRING,search:STRING'
        # Biqeury 읽기
        
        data_from_source = ( p 
        | 'Read Data' >> beam.io.Read(beam.io.BigQuerySource(
            query="SELECT * FROM analytics.history", use_standard_sql=True))
                               | 'Json dump' >> beam.Map(lambda x : json.dumps(x))
                               | 'Create DataFrames' >> beam.Map(dataframe_convert)
                               | 'Analysis To DataFrames' >> beam.Map(lambda x : parse_test(x))   # 분석하고 df -> json 됨
                               | 'Json load' >> beam.Map(lambda x : json.loads(x))
        )


        write_bigquery = ( data_from_source | 'Write To Bigquery' >>  beam.io.WriteToBigQuery(
                                                    project=project_id,
                                                    dataset=dataset_id,
                                                    table=table_id,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
                                                    
        )

    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()