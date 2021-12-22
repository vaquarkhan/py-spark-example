import random
import datetime
from kafka import KafkaProducer
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver import Chrome
from selenium.webdriver.common.keys import Keys
import requests
import time
import json
import sys
import re



options = webdriver.ChromeOptions()
options.add_argument('headless')
options.add_argument('window-size=1920x1080')
options.add_argument("disable-gpu")



channel = 'WIP3-woodlU'
url = 'https://www.youtube.com/watch?v='+ channel+'&list=RDpC6tPEaAiYU&start_radio=1'
# bootstrap_servers = ['35.209.78.194:9092'] # kafka broker ip
# topicName = 'topic-v1'

# bootstrap_servers = ['35.192.19.169:9092','34.121.204.65:9092','34.69.18.55:9092','35.224.115.84:9092'] # kafka broker ip
bootstrap_servers = ['34.122.143.4:9092']


topicName = 'test'
producer = KafkaProducer(bootstrap_servers = bootstrap_servers)




# num_scroll = int(input('스크롤 횟수 지정 : '))
num_scroll = 45

browser = Chrome(options=options)
browser.get(url)
browser.implicitly_wait(5)


body = browser.find_element_by_tag_name('body')


print("scroll down start.....")

while num_scroll:
    body.send_keys(Keys.PAGE_DOWN)
    time.sleep(0.11)
    num_scroll -= 1

html = browser.page_source
browser.quit()
bsObj = BeautifulSoup(html, 'lxml')
comment = bsObj.findAll('yt-formatted-string', attrs={'class': 'style-scope ytd-comment-renderer'})



print("kafka start.......")

for i in range(len(comment)):
    test = comment[i].text
    # data = bytes(str(test), 'utf-8')
    # print(test)
    producer.send(topicName, str(test).encode())
    time.sleep(1)

browser.quit()

