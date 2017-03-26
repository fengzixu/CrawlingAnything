#coding:utf8

from bs4 import BeautifulSoup
import urllib2
import datetime
import pymongo
import time
import sys
reload(sys)
sys.setdefaultencoding('utf8')

mongoClient = pymongo.MongoClient('localhost', 27017)
db = mongoClient.WechatIndex


def PullFormatHtmlData(tag, startCount):
    topUrl = 'https://' + tag + '.douban.com/top250?start=' + str(startCount)
    print '正在爬取' + tag + '分类下Top250数据，当前url为' + topUrl
    req = urllib2.Request(url=topUrl)
    res = urllib2.urlopen(req)
    soup = BeautifulSoup(res, "html.parser")
    return soup

def StoreDataToDb(coll, name):
    try:
        dbItem = {'name': name, 'time': datetime.datetime.utcnow()}
        coll.insert_one(dbItem)
    except pymongo.errors.DuplicateKeyError:
        print "insert duplicate data"
        pass

def CrawingMovieTop250Data(tag, coll):
    startCount = 0
    for i in range (0, 10):
        time.sleep(2) 
        soup = PullFormatHtmlData(tag, startCount)
        grids = soup.find('ol', {'class': 'grid_view'})
        lists = grids.find_all('li')
        for item in lists:
            details = item.find('div', {'class': 'hd'})
            titles = details.find_all('span', {'class': 'title'})
            if len(titles) >= 1:
                print 'get top data, name:', titles[0].get_text()
                StoreDataToDb(coll, titles[0].get_text())
            else:
                print 'titles is null. skip this'
        startCount+=25

def CrawingMusicTop250Data(tag, coll):
    startCount = 0
    for i in range(0, 10):
        time.sleep(2)
        soup = PullFormatHtmlData(tag, startCount)
        indentTag = soup.find('div', {'class': 'indent'})
        tables = indentTag.find_all('table')
        for table in tables:
            musicName = table.find('a').get('title')
            musicName = musicName.strip().split('-')[1]
            StoreDataToDb(coll, musicName)
            print 'get top data, name', musicName
        startCount+=25

def CrawingBookTop250Data(tag, coll):
    startCount = 0
    for i in range (0, 10):
        time.sleep(2)
        soup = PullFormatHtmlData(tag, startCount)
        article = soup.find('div', {'class': 'article'})
        bookList = article.find_all('table')
        for bookItem in bookList:
            bookInfo = bookItem.find('div', {'class': 'pl2'})
            bookName = bookInfo.find('a').get('title')
            if bookName == None or len(bookName) <= 0:
                continue
            StoreDataToDb(coll, bookName)
            print 'get top book data, name', bookName
        startCount+=25

top250Movie = db.top250movie
top250Movie.create_index("name", unique=True)
top250Music = db.top250music
top250Music.create_index("name", unique=True)
top250Book = db.top250book
top250Book.create_index("name", unique=True)

CrawingMovieTop250Data('movie', top250Movie)
print '======================豆瓣电影Top250数据爬取完毕==========================='
CrawingMusicTop250Data('music', top250Music)
print '======================豆瓣音乐Top250数据爬取完毕==========================='
CrawingBookTop250Data('book', top250Book)
print '======================豆瓣阅读Top250数据爬取完毕==========================='
