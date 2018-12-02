from bs4 import BeautifulSoup
import urllib.request
import urllib.parse
import pandas as pd
import json
import re
import datetime
from multiprocessing import Queue, Process, Manager
import pickle
import log
import check_overlap_url



class jongdal:
    def __init__(self, depth):
        self.depth = int(depth)
        self.conf = self.get_cof()
        crawl_url_list = self.get_seed()
        for url in crawl_url_list:
            self.file_list = []
            self.set_data = []
            self.working_count = 0
            self.make_dict(url)
            self.get_domain(url)
            check_overlap_url.clear_url_lsit(self.domain_list[0])
            self.inject_url()
            for d in range(self.depth):
                self.working_count += 1
                self.make_url_list(url)
                for seed in self.domain_list:
                    check_overlap_url.completed_url_save(domain=seed, url_list=self.url_list[seed]['documents'])
                self.url_parser()

    def make_url_list(self,url):
        '''
        크롤링할 url의 리스트를 제작합니다.
        '''
        self.parse_url_list = []
        for domain in self.domain_list:
            ncompleted_list = check_overlap_url.check_overlap_url(domain=domain,url_list=self.url_list[str(domain)]['documents'])
            for url in ncompleted_list:
                self.parse_url_list.append(url)

    def process_starter(self):
        for p in self.processes:
            p.start()
        for p in self.processes:
            p.join(6)

        self.processes = []

    def save_sub_db(self,l):
        with open('sub_db.pickle', 'ab') as f:
            for a in l:
                pickle.dump(a, f)

    def save_filesub_db(self,l):
        with open('file_sub_db.pickle', 'ab') as f:
            for a in l:
                pickle.dump(a, f)

    def clear_sub_db(self):
        with open('sub_db.pickle', 'wb+') as f:
            pickle.dump("\n",f)

    def set_save_data(self,data):
        print(data)
        for i in data:
            if i["fileUrl"] not in self.file_list:
                self.file_list.append(i["fileUrl"])
                self.set_data.append(i)


        return self.set_data

    def fileinfo_save(self):
        f = open('file_sub_db.pickle', 'rb')
        url_file_list = []
        while 1:
            try:
                u = pickle.load(f)
                url_file_list.append(u)
            except EOFError:
                break

        with open(str(self.domain_list[0]) + '_file.json', 'w+', encoding="utf-8") as f:
            json.dump(self.set_save_data(url_file_list),f,ensure_ascii=False, indent="\t")

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
            self.url_list[seed]['end_date'] = str(datetime.datetime.now())
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
            parsing_url_list = manager.list()
            file_url_list = manager.list()
            for parse_url in self.parse_url_list:
                if self.q.qsize() > 30:
                    self.process_starter()
                if len(parsing_url_list) > 1000:
                    self.save_sub_db(parsing_url_list)
                    parsing_url_list = manager.list()
                    #file_url_list = manager.list()
                p = Process(target=self.parsing_url, args=(parse_url,parsing_url_list,file_url_list))
                self.q.put(parse_url)
                self.processes.append(p)

            self.process_starter()
            self.save_sub_db(parsing_url_list)
            self.save_filesub_db(file_url_list)

        self.fileinfo_save()
        self.sub_db_to_json()


    def connect_url(self,url):
        logger = log.jlog()
        hdr = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3', 'Accept-Encoding': 'none',
            'Accept-Language': 'en-US,en;q=0.8', 'Connection': 'keep-alive'}

        try:
            req = urllib.request.Request(url, headers=hdr)
            with urllib.request.urlopen(req) as response:
                html = response.read()
                soup = BeautifulSoup(html, 'html.parser')
                parsing_url_html = soup.find_all()
                return parsing_url_html

        except Exception as e:
            logger.error(str(e))

    def data_parser(self,fileurl,introurl,domain):
        if '.pdf' in fileurl or '.PDF' in fileurl:
            ftype = "pdf"
        elif '.doc' in fileurl or '.DOC' in fileurl:
            ftype = "doc"
        elif '.xls' in fileurl or '.XLS' in fileurl:
            ftype = "xls"
        elif '.ppt' in fileurl or '.PPT' in fileurl:
            ftype = "pppt"

        file_default = {
            "host": domain,
            "fileType": ftype,
            "introUrl": introurl,
            "fileUrl": fileurl,
            "fileSrc": fileurl,
            "regDate": str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            "recentWorkDate": str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            "category": "",
            "title": "",
            "author": "",
            "country": "",
            "summery": "",
            "organization": "",
            "pubDate": "",
            "pages": ""
        }
        return file_default


    def parsing_url(self,parse_url,l,file_url_list):
        '''
        전달받은 html문서에서 seed.txt에 입력된 사이트에 해당이 되는 url을 파싱하여 저장합니다.
        '''
        logger = log.jlog()
        if((('.PDF' or'.MP4' or'.DOC' or'.docx' or'.pdf' or'.jpg'or'.bmp'or'.jpeg'or'.mp4'or'.doc'or'.exe'or'.pptx'
             or'.png'or'.mp3'or'.doc'or'.docx'or'.ppt'or'.zip'or'.tar.gz'or'.rar'or'.alz'or'.az'or'.7zip'or'.tar'or
             '.iso'or'.wmf'or'.WMF'or'.csv'or'.xls'or'.GIF'or'.gif'or'.exe') not in parse_url)):
            logger.info(" Working depth : "+str(self.working_count)+str(" | start parsing url : "+parse_url))
            try:
                for i in self.connect_url(parse_url):
                    try:
                        for seed in self.domain_list:
                            if(i['href'][0] == '/' and seed in parse_url ):
                                for apend in self.full_domain_url_list:
                                    if seed in apend:
                                        if (('.pdf' or '.DOC' or '.docx' or '.pdf' or '.PDF' or '.doc' or '.docx' or '.ppt' or '.doc' or '.xls' or '.xlsx' or '.XLS' or '.XLSX') in \
                                                i['href']):
                                            file_url_list.append(self.data_parser(i['href'], parse_url, seed))
                                        l.append(apend+str("/")+i['href'][1:])

                            if (seed in i['href']) and (i not in self.url_list[seed]['documents']):
                                if (('.pdf' or '.DOC' or '.docx' or '.pdf' or '.PDF' or '.doc' or '.docx' or '.ppt' or '.doc' or '.xls' or '.xlsx' or '.XLS' or '.XLSX') in \
                                        i['href']):
                                    file_url_list.append(self.data_parser(i['href'], parse_url, seed))
                                l.append(i['href'])
                    except:
                        continue
            except Exception as e:
                logger.error(str(e))
            logger.info(" Working depth : "+str(self.working_count)+str(" | end parsing url : "+parse_url))
            self.q.get()
        else:
            logger.info(str(parse_url)+"is file url")

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

    def make_dict(self,url):
        '''
        json에 저장하기위한 dict형식의 변수를 생성합니다.
        '''
        self.url_list = {}
        self.url_file_list = {str(re.sub('\n|\t|\r|https://|www.|http://|/','',url))}
        self.url_list[re.sub('\n|\t|\r|https://|www.|http://|/','',url)]={'title':'','url':re.sub('\n','',url),'start_data':str(datetime.datetime.now()),'end_date':str(datetime.datetime.now()),'depth':self.working_count,'documents':[re.sub('\n','',url)]}

    def get_domain(self,url):
        '''
        seed에입력된 사이트에 해당되는 사이트만을 거르기위해 실행됩니다.
        '''
        self.domain_list = list(self.url_list.keys())
        self.full_domain_url_list = []
        for i in self.domain_list:
            self.full_domain_url_list.append(self.url_list[i]['url'])

    def inject_url(self):
        '''
        파싱한 url을 json형태로 저장합니다.
        '''
        with open(str(self.domain_list[0])+'.json', 'w+', encoding="utf-8") as make_file:
            json.dump(self.url_list, make_file, ensure_ascii=False, indent="\t")
