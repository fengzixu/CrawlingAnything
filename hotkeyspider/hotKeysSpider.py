#!/usr/bin/env python 
# -*-coding:utf-8 -*-

import urllib2
from bs4 import BeautifulSoup
import MySQLdb 
import xlrd


conn= MySQLdb.connect(
        host='localhost',
        port = 3306,
        user='root',
        db ='HotKeysDB',
        charset="utf8"
        )
cur = conn.cursor()

carStaticUrl = 'http://www.autohome.com.cn/grade/carhtml/%s.html'
carProvider = '汽车之家'
stockStaticUrl = 'http://vip.stock.finance.sina.com.cn/usstock/ustotal.php'
stockProvider = '新浪财经'
phoneProvider = '本地文件'
upperCharactors = [chr(i).upper() for i in range(97,123)]
createTableStatement = """CREATE TABLE `t_index_collect_word` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `word` varchar(32) NOT NULL COMMENT '关键词',
  `type` tinyint(17) NOT NULL DEFAULT '0' COMMENT '类型',
  `state` tinyint(1) NOT NULL DEFAULT '0' COMMENT '状态，0为未进行爬虫，1为已爬虫',
  `source` varchar(50) DEFAULT '' COMMENT '关键词来源',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8"""

#cur.execute(createTableStatement)
#conn.commit()

def InsertDataToDB(name, type_, state, source):
   cur.execute("insert into t_index_collect_word (word,type,state,source) values(%s,%s,%s,%s)",(name, type_, state, source))
   conn.commit()

def GetHotKeysForxlsFile():
    book = xlrd.open_workbook("phone.xlsx")
    sheet = book.sheet_by_index(0)
    phoneList = sheet.col_values(0)
    for phoneName in phoneList:
        if isinstance(phoneName,float):
            phoneName = int(phoneName)
        InsertDataToDB(phoneName, 8, 0, phoneProvider)

def GetHotKeysForStock():
    content = urllib2.urlopen(url=stockStaticUrl)
    soup = BeautifulSoup(content, "html.parser")
    stockList = soup.find('div', {'class': 'col_div'})
    print "正在爬取新浪财经美股数据"
    for stockInfo in stockList.find_all('a'):
        stockName = stockInfo.get_text()
        stockName = stockName[:stockName.find('(')]
        InsertDataToDB(stockName, 15, 0, stockProvider)
    return
def GetHotKeysForCar():
    for i, ch in enumerate(upperCharactors, 0):
        pullUrl = carStaticUrl % ch
        print "正在爬取汽车分类下第%s页的数据. Url为%s" % (ch, pullUrl)
        content = urllib2.urlopen(url=pullUrl)
        soup = BeautifulSoup(content, "html.parser", from_encoding="gb18030")
        dtList = soup.find_all('dt')
        for carInfo in dtList:
            d = carInfo.find('div')
            link = d.find('a')
            carName = link.get_text()
            InsertDataToDB(carName, 7, 0, carProvider)
#GetHotKeysForStock()
#GetHotKeysForCar()
GetHotKeysForxlsFile()
cur.close()
conn.close()

