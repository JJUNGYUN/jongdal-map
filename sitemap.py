from bs4 import BeautifulSoup
import urllib.request
import urllib.parse
import pandas as pd
import re
from xml.etree.ElementTree import parse
url_set = []

def url_parser(url,conf):
    url_list = []
    for i in url:
        print("crawl start" ,i)
        try:
            with urllib.request.urlopen(i) as response:
                html = response.read()
                soup = BeautifulSoup(html, 'html.parser')
                parse_url = soup.find_all()
                parsing_url(parse_url,conf,url_list)
        except:
            continue
    inject_url(url_list)

def parsing_url(parse_html,conf,url_list):
    if conf['external_url_accept'] == 'True':
        for i in parse_html:
            try:
                if 'http' in i['href'] and i not in url_list:
                    url_list.append(i['href'])
            except:
                continue
    else:
        for i in parse_html:
            try:
                if 'http://www.inven.co.kr/' in i['href'] and i not in url_list:
                    url_list.append(i['href'])
            except:
                continue





def get_cof():
    conf = pd.read_json('conf.json', typ='series')

    return conf

def get_seed():
    f = open('seed.txt','r')
    return f

def inject_url(urls):
    f = open('url_db.txt','w')
    for i in urls:
        f.write(i+"\r\n")

def get_url_db():
    f = open('url_db.txt','r')
    db_list = []
    for i in f:
        db_list.append(i)
    return db_list
if __name__ == '__main__':
    conf = get_cof()
    inject_url(get_seed())
    #for i in range(2):
    #print('start depth : %d',i)
    url_parser(get_url_db(), conf)
