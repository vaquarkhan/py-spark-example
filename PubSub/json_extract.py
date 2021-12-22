import argparse
import logging
from random import *
import datetime as pydatetime
import time
import base64
import json



def get_now():
    return pydatetime.datetime.now()

def get_now_timestamp():
    return int(get_now().timestamp())



def test(num):
    for i in range(1,num):
        form = {
                "tags" : "iot_data" ,
                "data" : {
                    "timestamp" : get_now_timestamp(),
                    "x" : uniform(1,100),
                    "y" : uniform(1,100),
                    "temperature" : uniform(10,30)
                    }
                }
        time.sleep(1)
        
        
        json_body = json.dumps(form)
        data = bytes(json_body, 'utf8')
        print("byte 형태로 만들기")
        print(data)
        print(" ")


        test = data.decode(encoding ='utf8')
        record = json.loads(test)
        print("json 형태 풀기")
        print(record)
        print(" ")

        # bq 변환
        bq = {}
        bq['tags'] = record['tags']
        bq['x'] = record['data']['x']
        bq['y'] = record['data']['y']

        # bt 변환
        bt = {}
        bt['tags'] = record['tags']
        bt['timestamp'] = record['data']['timestamp']
        bt['temperature'] = record['data']['temperature']

        print("----bq----")
        print(bq)
        print("----bt----")
        print(bt)
        print("------------------------------------------------------------ ")

        


test(11)