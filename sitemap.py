from bs4 import BeautifulSoup
import urllib.request
import urllib.parse
import pandas as pd
import json
import re
import datetime
from multiprocessing import Queue, Process, Manager
import sys
import pickle
import time
class jongdal:
    def __init__(self, depth):
        self.working_count = 0
        self.depth = int(depth)
        self.conf = self.get_cof()
        self.make_dict()
        self.inject_url()
        self.get_domain()
        for i in range(self.depth):
            self.working_count += 1
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

    def process_starter(self):
        for p in self.processes:
            p.start()
        for p in self.processes:
            p.join()
        time.sleep(6)
        for i in self.processes:
            i.terminate()
        self.processes = []


    def save_sub_db(self,l):
        with open('sub_db.pickle', 'ab') as f:
            for a in l:
                pickle.dump(a, f)
    def clear_sub_db(self):
        with open('sub_db.pickle', 'wb+') as f:
            pickle.dump("\n",f)

    def sub_db_to_json(self):
        f = open('sub_db.pickle', 'rb')
        url_list =[]
        while 1:
            try:
                u = pickle.load(f)
                url_list.append(u)
            except EOFError:
                break

        for i in url_list:
            for seed in self.domain_list:
                if (seed in i):
                    self.url_list[seed]['documents'].append(i)
        for seed in self.domain_list:
            self.url_list[seed]['documents'] = list(set(self.url_list[seed]['documents']))
            self.url_list[seed]['depth'] = self.working_count
        self.inject_url()

    def url_parser(self):
        '''
        들어간 url을 bs4를 이용하여 html을 파싱합니다
        :return:
        '''
        self.q = Queue()
        self.processes = []
        self.clear_sub_db()
        with Manager() as manager:
            l = manager.list()
            for parse_url in self.parse_url_list:
                if self.q.qsize() > 30:
                    self.process_starter()

                if len(l) > 1000:
                    self.save_sub_db(l)
                    l = manager.list()
                p = Process(target=self.parsing_url, args=(parse_url,l))
                self.q.put(parse_url)
                self.processes.append(p)


            self.process_starter()
            self.save_sub_db(l)

        self.sub_db_to_json()

    def connect_url(self,url):
        hdr = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3', 'Accept-Encoding': 'none',
            'Accept-Language': 'en-US,en;q=0.8', 'Connection': 'keep-alive'}

        try:
            req = urllib.request.Request(url, headers=hdr);
            with urllib.request.urlopen(req) as response:
                html = response.read()
                soup = BeautifulSoup(html, 'html.parser')
                parsing_url_html = soup.find_all()
                return parsing_url_html

                # self.parsing_url(parsing_url_html,i)
        except Exception as e:
            print(e)

    def parsing_url(self,parse_url,l):
        '''
        전달받은 html문서에서 seed.txt에 입력된 사이트에 해당이 되는 url을 파싱하여 저장합니다.
        '''
        if((('.PDF' or'.MP4' or'.DOC' or'.docx' or'.pdf' or'.jpg'or'.bmp'or'.jpeg'or'.mp4'or'.doc'or'.exe'or'.pptx'or'.png'or'.mp3'or'.doc'or'.docx'or'.ppt'or'.zip'or'.tar.gz'or'.rar'or'.alz'or'.az'or'.7zip'or'.tar'or'.iso'or'.wmf'or'.WMF'or'.csv'or'.xls'or'.GIF'or'.gif'or'.exe') not in parse_url)):
            print(datetime.datetime.now()," start parsing url : ", parse_url)
            try:
                for i in self.connect_url(parse_url):
                    try:
                        for seed in self.domain_list:
                            if(i['href'][0] == '/' and seed in parse_url ):
                                for apend in self.full_domain_url_list:
                                    if seed in apend:
                                        l.append(apend+i['href'][1:])

                            if (seed in i['href']) and (i not in self.url_list[seed]['documents']):
                                l.append(i['href'])

                    except:
                        continue
            except Exception as e:
                print(e)
            print(datetime.datetime.now()," end parsing url : ", parse_url)
            self.q.get()
        else:
            print('Reject')
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
            self.url_list[re.sub('\n|\t|\r|https://|www.|http://|/','',i)]={'title':'','url':re.sub('\n','',i),'data':str(datetime.datetime.now()),'depth':self.working_count,'documents':[re.sub('\n','',i)]}

    def get_domain(self):
        '''
        seed에입력된 사이트에 해당되는 사이트만을 거르기위해 실행됩니다.
        '''
        self.domain_list = list(self.url_list.keys())
        self.full_domain_url_list = []
        for i in self.domain_list:
            self.full_domain_url_list.append(self.url_list[i]['url'])
        print(self.full_domain_url_list)

    def inject_url(self):
        '''
        파싱한 url을 json형태로 저장합니다.
        '''
        with open('url_db.json', 'w+', encoding="utf-8") as make_file:
            json.dump(self.url_list, make_file, ensure_ascii=False, indent="\t")

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('python sitemap.py [depth]')
    depth  = int(sys.argv[1])
    jongdal(depth=sys.argv[1])

