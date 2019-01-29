# coding=utf-8
# coding=utf-8
import json
import re
import random
import datetime
import time

import pymongo
import requests
from bs4 import BeautifulSoup

connect = pymongo.MongoClient('localhost', 27017)
doubanDB = connect['douban']
# doubanCollection = doubanDB['tvplay_sample']
movie_sample_col = doubanDB['movie_sample']
movie_info_col = doubanDB['movie_info']

ip_ports = []

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
    return random.choice(USER_AGENTS)


def log_record(status_code, url):
    # 日志
    f = open('data/info_record2018.txt', 'a', encoding='utf-8')
    f.write(str(status_code) + ',' + url + ',' + str(datetime.datetime.now()) + '\n')
    f.close()

def extract_movie_info(page_html):
    soup = BeautifulSoup(page_html, "lxml")

    #页面中script中的json信息
    script_list = soup.find_all('script', type='application/ld+json')
    movie_json_info = {}
    if len(script_list) == 1:
        try:
            movie_json_info = script_list[0].text
            movie_json_info = json.loads(movie_json_info)
        except:
            movie_json_info = {}

     #解析html中的信息
    content_soup = soup.find_all('div', id='content')
    # print(content_soup)
    if len(content_soup) == 1:
        content_soup = content_soup[0]
        try:
            movie_name = content_soup.find('span', property="v:itemreviewed").text
        except:
            movie_name = ''

        try:
            movie_year = content_soup.find('span', class_="year").text.replace('(', '').replace(')', '')
        except:
            movie_year = ''

        movie_info_soup = content_soup.find('div', id="info")
        try:
            director_list = movie_info_soup.find('span', text='导演').next_sibling.next_sibling.text.replace(' ', '').split('/')
        except:
            director_list = []

        try:
            author_list = movie_info_soup.find('span', text='编剧').next_sibling.next_sibling.text.replace(' ', '').split('/')
        except:
            author_list = []

        try:
            actor_list = movie_info_soup.find('span', text='主演').next_sibling.next_sibling.text.replace(' ', '').split('/')
        except:
            actor_list = []

        try:
            type_soup = movie_info_soup.find('span', text='类型:').find_next_siblings("span")
            type_list = []
            for x in type_soup:
                if x.attrs.get('class'):
                    break
                type_list.append(x.text)
        except:
            type_list = []

        try:
            state_list = movie_info_soup.find('span', text='制片国家/地区:').next_sibling.replace(' ', '').split('/')
        except:
            state_list = []
        try:
            language_list = movie_info_soup.find('span', text='语言:').next_sibling.replace(' ', '').split('/')
        except:
            language_list = []

        try:
            release_date_soup = movie_info_soup.find('span', text='上映日期:').find_next_siblings("span")
            release_date_list = []
            for x in release_date_soup:
                if x.attrs.get('class'):
                    break
                release_date_list.append(x.text)
        except:
            release_date_list = []

        try:
            duration = movie_info_soup.find('span', text='片长:').find_next_sibling("span").text
        except:
            duration = ''

        try:
            another_name_list = movie_info_soup.find('span', text='又名:').next_sibling.replace(' ', '').split('/')
        except:
            another_name_list = []
        # print('movie_name:', movie_name)
        # print('movie_year:', movie_year)
        # print('director:', director_list)
        # print('author:', author_list)
        # print('actor:', actor_list)
        # print('type:', type_list)
        # print('state:', state_list)
        # print('language:', language_list)
        # print('release_date:', release_date_list)
        # print('duration:', duration)
        # print('another_name:', another_name_list)
        try:
            rate_num = content_soup.find('strong', class_="rating_num").text
        except:
            rate_num = 0
        try:
            rate_people = content_soup.find('a', class_="rating_people").text.replace('人评价', '')
        except:
            rate_people = 0
        try:
            star_pre_soup = content_soup.find_all('span', class_="rating_per")
            if len(star_pre_soup) == 5:
                star5_pre = star_pre_soup[0].text
                star4_pre = star_pre_soup[1].text
                star3_pre = star_pre_soup[2].text
                star2_pre = star_pre_soup[3].text
                star1_pre = star_pre_soup[4].text
            else:
                star5_pre = 0
                star4_pre = 0
                star3_pre = 0
                star2_pre = 0
                star1_pre = 0
        except:
            star5_pre = 0
            star4_pre = 0
            star3_pre = 0
            star2_pre = 0
            star1_pre = 0

        try:
            recommendations_soup = content_soup.find('div', class_="recommendations-bd").find_all('a')
            recommendations_list = []
            for recommendation in recommendations_soup:
                if recommendation.find('img'):
                    href = recommendation.attrs['href']
                    name = recommendation.find('img').attrs['alt']
                    recommendations_list.append({'name': name, 'href': href})
        except:
            recommendations_list = []

        # print('rate_num:', rate_num)
        # print('rate_people:', rate_people)
        # print('star5_pre:', star5_pre)
        # print('star4_pre:', star4_pre)
        # print('star3_pre:', star3_pre)
        # print('star2_pre:', star2_pre)
        # print('star1_pre:', star1_pre)
        # print('recommendations:', recommendations_list)

        try:
            comments_num = re.sub('\D', '', content_soup.find('div', id='comments-section').find('a', text=re.compile('全部')).text)
        except:
            comments_num = 0
        try:
            reviews_num = re.sub('\D', '', content_soup.find('a', href='reviews').text)
        except:
            reviews_num = 0
        try:
            tags_soup = content_soup.find('div', class_='tags-body').find_all('a')
            tag_list = []
            for tag in tags_soup:
                tag_list.append(tag.text)
        except:
            tag_list = []
        try:
            interests_soup = content_soup.find('div', class_='subject-others-interests-ft').find_all('a')
            if len(interests_soup) == 2:
                watched_peolpe = re.sub('\D', '',interests_soup[0].text)
                interest_peolpe = re.sub('\D', '',interests_soup[1].text)
            else:
                watched_peolpe = 0
                interest_peolpe = 0
        except:
            watched_peolpe = 0
            interest_peolpe = 0
        try:
            summary = content_soup.find('span', property='v:summary').text.replace(' ', '').replace('\n', '').replace('\u3000', '')
        except:
            summary = ''
        # print('comments:', comments_num)
        # print('reviews:', reviews_num)
        # print('tags:', tag_list)
        # print('watched_peolpe:', watched_peolpe)
        # print('interest_peolpe:', interest_peolpe)

        movie_info_dict = {}
        movie_info_dict['json_info'] = movie_json_info
        movie_info_dict['movie_name'] = movie_name
        movie_info_dict['movie_year'] = movie_year
        movie_info_dict['director'] = director_list
        movie_info_dict['author'] = author_list
        movie_info_dict['actor'] = actor_list
        movie_info_dict['type'] = type_list
        movie_info_dict['state'] = state_list
        movie_info_dict['language'] = language_list
        movie_info_dict['release_date'] = release_date_list
        movie_info_dict['duration'] = duration
        movie_info_dict['another_name'] = another_name_list
        movie_info_dict['rate_num'] = rate_num
        movie_info_dict['rate_people'] = rate_people
        movie_info_dict['star5_pre'] = star5_pre
        movie_info_dict['star4_pre'] = star4_pre
        movie_info_dict['star3_pre'] = star3_pre
        movie_info_dict['star2_pre'] = star2_pre
        movie_info_dict['star1_pre'] = star1_pre
        movie_info_dict['recommendations'] = recommendations_list
        movie_info_dict['comments_num'] = comments_num
        movie_info_dict['reviews_num'] = reviews_num
        movie_info_dict['tag'] = tag_list
        movie_info_dict['watched_peolpe'] = watched_peolpe
        movie_info_dict['interest_peolpe'] = interest_peolpe
        movie_info_dict['summary'] = summary
        # print(movie_info_dict)

        return movie_info_dict


def get_ip_port():
    global ip_ports
    if len(ip_ports) < 3:
        ip_ports = json.loads(
            requests.get('http://127.0.0.1:8000/?types=0&protocol=2&count=5&country=%E5%9B%BD%E5%86%85').text)
            # requests.get('http://127.0.0.1:8000/?types=0&protocol=1&count=5').text)
        if len(ip_ports) < 2:
            ip_ports = json.loads(
                requests.get('http://127.0.0.1:8000/?types=0&protocol=1&count=5&country=%E5%9B%BD%E5%86%85').text)

    return random.choice(ip_ports)


def get_movie_html(url):
    global ip_ports
    agent = get_agent()
    headers = {
        'Host': 'movie.douban.com',
        'referer': 'https://movie.douban.com/',
        'user-agent': agent
    }
    # proxies_list = [
    #     # {"https": "110.52.235.204:9999"},
    #     {'https': '171.12.112.33:9999'},
    #     # {'https': '110.52.235.7:9999'}
    # ]
    # ip_ports = json.loads(requests.get('http://127.0.0.1:8000/?types=0&protocol=1&count=5&country=%E5%9B%BD%E5%86%85').text)
    isSuccess = False
    ip_port = get_ip_port()
    while not isSuccess:
        if ip_port[2] == 0:
            continue
        ip = ip_port[0]
        port = ip_port[1]
        proxies = {
            'https': '%s:%s' % (ip, port)
        }
        print(ip_port)
        try:
            response = requests.get(url, headers=headers, proxies=proxies, timeout=10)
            # print("length:",len(response.text))
            if len(response.text) < 1000:
                # response = requests.get(url, headers=headers, proxies=proxies, timeout=10)
                print('length:', len(response.text))
                raise NameError('ip error!')
            isSuccess = True
            if ip_port[2] < 5:
                for x in ip_ports:
                    if x[0] == ip_port[0]:
                        x[2] = 10
        except:
            ip_port[2] -= 2
            new_ip_ports = []
            for x in ip_ports:
                if x[2] < 1:
                    delete_ip = x[0]
                    requests.get('http://127.0.0.1:8000/delete?ip=' + delete_ip)
                else:
                    new_ip_ports.append(x)
            if len(new_ip_ports) < 3:
                ip_ports = json.loads(requests.get('http://127.0.0.1:8000/?types=0&protocol=2&count=5&country=%E5%9B%BD%E5%86%85').text)
                if len(ip_ports) < 2:
                    ip_ports = json.loads(
                        requests.get(
                            'http://127.0.0.1:8000/?types=0&protocol=1&count=5&country=%E5%9B%BD%E5%86%85').text)
            else:
                ip_ports = new_ip_ports
            ip_port = random.choice(ip_ports)
            print("try again")

    print(url, response.status_code)
    log_record(response.status_code, url)
    if response.status_code == 404:
        return response.status_code
    if response.status_code != 200:
        return None
    return response.text


def get_movie_sample_list(year):
    movie_sample_cursor = movie_sample_col.find({'year': year})
    movie_sample_list = []
    for movie_sample in movie_sample_cursor:
        movie_info = {}
        if movie_sample.get('isCrawl') != None:
            if movie_sample.get('isCrawl') == 1 or movie_sample.get('isCrawl') == 404:
                continue
        movie_info['name'] = movie_sample['title']
        movie_info['id'] = movie_sample['id']
        movie_info['url'] = movie_sample['url']
        movie_info['year'] = movie_sample['year']
        movie_sample_list.append(movie_info)
    print(len(movie_sample_list))
    return movie_sample_list


def save_in_movie_sample_col(movie_id):
    crawl_time = datetime.datetime.now()
    movie_sample_col.update_one({'id': movie_id}, {'$set': {'isCrawl': 1, 'crawl_time': crawl_time}})


def save_movie(movie_info_dict):
    movie_info_col.insert_one(movie_info_dict)

    #在movie_sample中记录爬取状态与时间
    movie_id = movie_info_dict['id']
    save_in_movie_sample_col(movie_id)


def start_crawl(year):
    # year = 2017
    movie_sample_list = get_movie_sample_list(year)
    for movie_sample_info in movie_sample_list:
        print(movie_sample_info['id'], movie_sample_info['name'] + " start")
        movie_html = get_movie_html(movie_sample_info['url'])
        if movie_html == 404:
            continue
        if movie_html == None:
            break
        movie_info_dict = extract_movie_info(movie_html)
        if movie_info_dict == None:
            print('None', movie_sample_info['id'], movie_sample_info['name'], '****************')
            print(movie_info_dict)
            continue

        movie_info_dict['id'] = movie_sample_info['id']
        movie_info_dict['create_time'] = datetime.datetime.now()

        save_movie(movie_info_dict)
        print(movie_info_dict['movie_name'] + " complete")
        print("########")

        # time.sleep(random.randint(3, 5))
        # time.sleep(0.5)


if __name__ == "__main__":
    for year in [2010, 2009, 2008]:
        start_crawl(year)