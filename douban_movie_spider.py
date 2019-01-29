# coding=utf-8
import json
from random import choice
import datetime
import time

import pymongo
import requests

connect = pymongo.MongoClient('localhost', 27017)
doubanDB = connect['douban']
# doubanCollection = doubanDB['tvplay_sample']
doubanCollection = doubanDB['movie_sample']


def get_agent():
    USER_AGENTS = [
        "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; AcooBrowser; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Acoo Browser; SLCC1; .NET CLR 2.0.50727; Media Center PC 5.0; .NET CLR 3.0.04506)",
        "Mozilla/4.0 (compatible; MSIE 7.0; AOL 9.5; AOLBuild 4337.35; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
        "Mozilla/5.0 (Windows; U; MSIE 9.0; Windows NT 9.0; en-US)",
        "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)",
        "Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 1.0.3705; .NET CLR 1.1.4322)",
        "Mozilla/4.0 (compatible; MSIE 7.0b; Windows NT 5.2; .NET CLR 1.1.4322; .NET CLR 2.0.50727; InfoPath.2; .NET CLR 3.0.04506.30)",
        "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN) AppleWebKit/523.15 (KHTML, like Gecko, Safari/419.3) Arora/0.3 (Change: 287 c9dfb30)",
        "Mozilla/5.0 (X11; U; Linux; en-US) AppleWebKit/527+ (KHTML, like Gecko, Safari/419.3) Arora/0.6",
        "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.2pre) Gecko/20070215 K-Ninja/2.1.1",
        "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9) Gecko/20080705 Firefox/3.0 Kapiko/3.0",
        "Mozilla/5.0 (X11; Linux i686; U;) Gecko/20070322 Kazehakase/0.4.5",
        "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.8) Gecko Fedora/1.9.0.8-1.fc10 Kazehakase/0.5.6",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/535.20 (KHTML, like Gecko) Chrome/19.0.1036.7 Safari/535.20",
        "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; fr) Presto/2.9.168 Version/11.52",
        'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:22.0) Gecko/20100101 Firefox/22.0',
    ]
    return choice(USER_AGENTS)

def log_record(url):
    # 日志
    f = open('data/record2018.txt', 'a', encoding='utf-8')
    f.write(url + ',' + str(datetime.datetime.now()) + '\n')
    f.close()


def get_movie_list(url, year):
    print(url)
    stop_flag = False
    agent = get_agent()
    headers = {'user-agent': agent}
    response = requests.get(url, headers=headers, timeout=30)
    if response.status_code != 200:
        return True
    response_str = response.text
    response_dict = json.loads(response_str)
    movie_list = response_dict['data']
    no_rate_num = 0
    for movie_info in movie_list:
        if movie_info['rate'] == '' or movie_info['rate'] == None:
            no_rate_num += 1
            continue
        movie_info['year'] = year
        print(movie_info['title'], movie_info['rate'])
        doubanCollection.insert_one(movie_info)
    if no_rate_num > 10:
        stop_flag = True
    return stop_flag


def start_crawl(year):
    start_url = "https://movie.douban.com/j/new_search_subjects?sort=S&range=0,10&tags=%E7%94%B5%E5%BD%B1&start=20&year_range=2018,2018"
    stop_flag = False
    start_index = 120
    while not stop_flag:
        # url = 'https://movie.douban.com/j/new_search_subjects?sort=S&range=0,10&tags=电影&start={start_index}&year_range={start_year},{end_year}'
        # url = 'https://movie.douban.com/j/new_search_subjects?sort=S&range=0,10&tags=电影&start=%s&year_range=%s,%s' % (start_index, year, year)
        # url.format(start_index=start_index, start_year=year, end_year=year)
        url = 'https://movie.douban.com/j/new_search_subjects?sort=S&range=0,10&tags=电影&start=%s&year_range=%s,%s' % (start_index, year, year)
        log_record(url)
        stop_flag = get_movie_list(url, year)
        start_index +=20
        time.sleep(5)
        if start_index >5000:
            stop_flag = True


if __name__ == "__main__":
    # year = 2018
    for year in [2008, ]:
        start_crawl(year)