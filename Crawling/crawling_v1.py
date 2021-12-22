from bs4 import BeautifulSoup
from selenium import webdriver
import time

url = 'https://play.google.com/store/apps/details?id=kr.neogames.realfarm&showAllReviews=true'

driver = webdriver.Chrome('C:/Users/user/Desktop/Python/chromedriver_win32/chromedriver.exe')
driver.get(url)
driver.implicitly_wait(5)

errTime = 0 # 버튼 못찾음
successTime = 0 # 버튼 누른 횟수


while(True): # 첫번쨰 더보기 찾을때까지 내림
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(5)
    try:
        # 버튼 찾기
        element = driver.find_element_by_xpath('//*[contains(@class,"U26fgb O0WRkf oG5Srb C0oVfc n9lfJ")]')
        if(element is not None):
            driver.execute_script("arguments[0].click();",element)
            break
    except Exception:
        continue

while(errTime < 15 and successTime < 5):
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(5)
    try:   
        element = driver.find_element_by_xpath('//*[contains(@class,"U26fgb O0WRkf oG5Srb C0oVfc n9lfJ M9Bg4d")]')
        if(element is not None):
            driver.execute_script("arguments[0].click();",element)
            successTime+=1
            errTime = 0 # 더보기 버튼 눌러지면 errtime 0으로 초기화
    except Exception:
        errTime += 1

html = driver.page_source
driver.quit()

bsObj = BeautifulSoup(html, 'lxml')
div_review = bsObj.find_all("div", {"class" : "d15Mdf bAhLNe"})
print(len(div_review))