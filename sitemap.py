from bs4 import BeautifulSoup
import urllib.request
import urllib.parse
import pandas as pd
import json
import re
import datetime
from xml.etree.ElementTree import parse
import multiprocessing
import time

class jongdal:
    def __init__(self):
        self.conf = self.get_cof()
        self.make_dict()
        self.inject_url()

        self.get_domain()
        for i in range(2):
            self.make_url_list()
            self.url_parser()

    def make_url_list(self):
        '''
        크롤링할 url의 리스트를 제작합니다.
        :return:
        '''
        self.parse_url_list = []
        for domain in self.domain_list:
            for url in self.url_list[str(domain)]['documents']:
                self.parse_url_list.append(url)

    def url_parser(self):
        '''
        들어간 url을 bs4를 이용하여 html을 파싱합니다
        :return:
        '''
        pool = multiprocessing.Pool(processes=100)
        url_list = pool.map(self.parsing_url, self.parse_url_list)
        pool.close()
        pool.join()
        for i in url_list:
            for j in i:
                for seed in self.domain_list:
                    if (seed in j):
                        self.url_list[seed]['documents'].append(j)
        for seed in self.domain_list:
            self.url_list[seed]['documents'] = list(set(self.url_list[seed]['documents']))
        self.inject_url()

    def connect_url(self,url):
        try:
            with urllib.request.urlopen(url) as response:
                html = response.read()
                soup = BeautifulSoup(html, 'html.parser')
                parsing_url_html = soup.find_all()
                return parsing_url_html

                # self.parsing_url(parsing_url_html,i)
        except Exception as e:
            print(e)
    def parsing_url(self,parse_url):
        '''
        전달받은 html문서에서 seed.txt에 입력된 사이트에 해당이 되는 url을 파싱하여 저장합니다.
        '''
        print("start parsing url : ", parse_url)
        parse_list = []
        for i in self.connect_url(parse_url):
            try:
                for seed in self.domain_list:
                    if(i['href'][0] == '/' and seed in parse_url and (seed+i['href'][1:] not in self.url_list[seed]['documents'])):
                        parse_list.append(seed+i['href'][1:])
                        #self.url_list[seed]['documents'].append(seed+i['href'][1:])
                        #self.url_list[seed]['documents'] = list(set(url_list[seed]['documents']))
                    if (seed in i['href']) and (i not in self.url_list[seed]['documents']):
                        parse_list.append(i['href'])
                        #self.url_list[seed]['documents'].append(i['href'])
                        #self.url_list[seed]['documents'] = list(set(url_list[seed]['documents']))
            except:
                continue
        print("end parsing url : ", parse_url)

        return parse_list
    def get_cof(self):
        '''
        설정을 받아옵니다
        '''
        conf = pd.read_json('conf.json', typ='series')

        return conf

    def get_seed(self):
        '''
        seed.txt에 입력된 사이트들을 가져옵니다.
        '''
        f = open('seed.txt','r')
        return f

    def make_dict(self):
        '''
        json에 저장하기위한 dict형식의 변수를 생성합니다.
        '''
        self.url_list = {}
        for i in self.get_seed():
            self.url_list[re.sub('\n|\t|\r','',i)]={'title':'','url':re.sub('\n','',i),'data':str(datetime.datetime.now()),'documents':[re.sub('\n','',i)]}

    def get_domain(self):
        '''
        seed에입력된 사이트에 해당되는 사이트만을 거르기위해 실행됩니다.
        '''
        self.domain_list = list(self.url_list.keys())

    def inject_url(self):
        '''
        파싱한 url을 json형태로 저장합니다.
        '''
        with open('url_db.json', 'w+', encoding="utf-8") as make_file:
            json.dump(self.url_list, make_file, ensure_ascii=False, indent="\t")

if __name__ == '__main__':
    jongdal()