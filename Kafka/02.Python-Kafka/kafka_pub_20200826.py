import requests
import xmltodict, json
import pandas as pd
import os
import json
import time
from kafka import KafkaProducer


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:\gcloud_key/freud-int-200423-owner-88790c68f84a.json"

key = "hDMijoF1c3tWwfAX7CvXWx4XtGqpKpWbtDJkSuuHTy2ZntpEs3BfBTuEYFpKDvkWzkMXQ%2BwmHn%2Br6X4a7FoXHw%3D%3D"
url = "http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTrade?serviceKey={}&LAWD_CD=41135&DEAL_YMD=202008&".format(key)

content = requests.get(url).content
dict = xmltodict.parse(content)
jsonString = json.dumps(dict['response']['body']['items'], ensure_ascii=False)
jsonObj = json.loads(jsonString)

for item in jsonObj['item']:
    item['price'] = item.pop('거래금액')
    item['build_year'] = item.pop('건축년도')
    item['year'] = item.pop('년')
    item['dong'] = item.pop('법정동')
    item['apartment'] = item.pop('아파트')
    item['month'] = item.pop('월')
    item['day'] = item.pop('일')
    item['area'] = item.pop('전용면적')
    item['jibun'] = item.pop('지번')
    item['code'] = item.pop('지역코드')
    item['floor'] = item.pop('층')

for item in jsonObj['item']:
    item['price'] = item['price'].replace(",","")

bootstrap_servers = ['34.122.143.4:9092']
topicName = 'test'
producer = KafkaProducer(bootstrap_servers = bootstrap_servers)



for item in jsonObj['item']:
    data = json.dumps(item)
    producer.send(topicName, str(data).encode())
    time.sleep(1)
    print(data)



