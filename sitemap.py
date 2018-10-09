from bs4 import BeautifulSoup
import urllib.request
import urllib.parse
import pandas as pd
import json
import re
import datetime
from xml.etree.ElementTree import parse

class jongdal:
    def __init__(self):
        self.conf = self.get_cof()
        self.make_dict()
        self.inject_url()
        self.get_domain()
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
        for i in self.parse_url_list:
            print("crawl start" ,i)
            try:
                with urllib.request.urlopen(i) as response:
                    html = response.read()
                    soup = BeautifulSoup(html, 'html.parser')
                    parse_url = soup.find_all()
                    self.parsing_url(parse_url)
            except:
                continue
        self.inject_url()

    def parsing_url(self,parse_html):
        '''
        전달받은 html문서에서 seed.txt에 입력된 사이트에 해당이 되는 url을 파싱하여 저장합니다.
        '''
        for i in parse_html:
            try:
                for seed in self.domain_list:
                    if (seed in i['href']) and (i not in self.url_list[seed]['documents']):
                        self.url_list[seed]['documents'].append(i['href'])
            except:
                continue


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
            self.url_list[re.sub('/|http://www.|https://www.|\n','',i)]={'title':'','url':re.sub('\n','',i),'data':str(datetime.datetime.now()),'documents':[re.sub('\n','',i)]}

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