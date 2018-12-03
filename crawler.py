from bs4 import BeautifulSoup
import urllib.request
import urllib.parse
import pandas as pd
import json
import re
import datetime
from multiprocessing import Queue, Process, Manager
import pickle
import log_manage
import file_manage
import overlap_manage


def make_url_list(domain_list,url_list):
    '''
    크롤링할 url의 리스트를 제작합니다.
    '''
    parse_url_list = []
    for domain in domain_list:
        ncompleted_list = overlap_manage.check_overlap_url(domain=domain,
                                                              url_list=url_list[str(domain)]['documents'])
        for url in ncompleted_list:
            parse_url_list.append(url)

    return parse_url_list


def process_starter(processes):
    for p in processes:
        p.start()

    for p in processes:
        p.join(6)

    return []


def url_parser(parse_url_list,url_list,domain_list,full_domain_url_list,set_data,file_list,working_count):
    '''
    들어간 url을 bs4를 이용하여 html을 파싱합니다
    :return:
    '''
    q = Queue()
    processes = []
    file_manage.clear_sub_db()
    with Manager() as manager:
        parsing_url_list = manager.list()
        file_url_list = manager.list()
        print(parse_url_list)
        for parse_url in parse_url_list:
            if q.qsize() > 30:
                processes= process_starter(processes)
            if len(parsing_url_list) > 1000:
                file_manage.save_sub_db(parsing_url_list)
                parsing_url_list = manager.list()
                #file_url_list = manager.list()
            p = Process(target=parsing_url, args=(parse_url,parsing_url_list,file_url_list,url_list,domain_list,q,full_domain_url_list,working_count))
            q.put(parse_url)
            processes.append(p)

        processes = process_starter(processes)
        file_manage.save_sub_db(parsing_url_list)
        file_manage.save_filesub_db(file_url_list)

    file_manage.fileinfo_save(domain_list,set_data,file_list)
    file_manage.sub_db_to_json(domain_list,url_list,working_count)


def connect_url(url):
    logger = log_manage.jlog()
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


def data_parser(fileurl,introurl,domain):
    if '.pdf' in fileurl or '.PDF' in fileurl:
        ftype = "pdf"
    elif '.doc' in fileurl or '.DOC' in fileurl:
        ftype = "doc"
    elif '.xls' in fileurl or '.XLS' in fileurl:
        ftype = "xls"
    elif '.ppt' in fileurl or '.PPT' in fileurl:
        ftype = "pppt"
    else:
        ftype = "ERROR"

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


def parsing_url(parse_url,l,file_url_list,url_list,domain_list,q,full_domain_url_list,working_count):
    '''
    전달받은 html문서에서 seed.txt에 입력된 사이트에 해당이 되는 url을 파싱하여 저장합니다.
    '''
    logger = log_manage.jlog()
    if((('.PDF' or'.MP4' or'.DOC' or'.docx' or'.pdf' or'.jpg'or'.bmp'or'.jpeg'or'.mp4'or'.doc'or'.exe'or'.pptx'
         or'.png'or'.mp3'or'.doc'or'.docx'or'.ppt'or'.zip'or'.tar.gz'or'.rar'or'.alz'or'.az'or'.7zip'or'.tar'or
         '.iso'or'.wmf'or'.WMF'or'.csv'or'.xls'or'.GIF'or'.gif'or'.exe') not in parse_url)):
        logger.info(" Working depth : "+str(working_count)+str(" | start parsing url : "+parse_url))
        try:
            for i in connect_url(parse_url):
                try:
                    for seed in domain_list:
                        if(i['href'][0] == '/' and seed in parse_url ):
                            for apend in full_domain_url_list:
                                if seed in apend:
                                    if (('.pdf' or '.DOC' or '.docx' or '.pdf' or '.PDF' or '.doc' or '.docx' or '.ppt' or '.doc' or '.xls' or '.xlsx' or '.XLS' or '.XLSX') in \
                                            i['href']):
                                        file_url_list.append(data_parser(i['href'], parse_url, seed))
                                    l.append(apend+str("/")+i['href'][1:])

                        if (seed in i['href']) and (i not in url_list[seed]['documents']):
                            if (('.pdf' or '.DOC' or '.docx' or '.pdf' or '.PDF' or '.doc' or '.docx' or '.ppt' or '.doc' or '.xls' or '.xlsx' or '.XLS' or '.XLSX') in \
                                    i['href']):
                                file_url_list.append(data_parser(i['href'], parse_url, seed))
                            l.append(i['href'])
                except:
                    continue
        except Exception as e:
            logger.error(str(e))
        logger.info(" Working depth : "+str(working_count)+str(" | end parsing url : "+parse_url))
        q.get()
    else:
        logger.info(str(parse_url)+"is file url")


def make_dict(url,working_count):
    '''
    json에 저장하기위한 dict형식의 변수를 생성합니다.
    '''
    url_list = {}
    url_file_list = {str(re.sub('\n|\t|\r|https://|www.|http://|/','',url))}
    url_list[re.sub('\n|\t|\r|https://|www.|http://|/','',url)]={'title':'','url':re.sub('\n','',url),'start_data':str(datetime.datetime.now()),'end_date':str(datetime.datetime.now()),'depth':working_count,'documents':[re.sub('\n','',url)]}

    return url_list,url_file_list


def get_domain(url_list):
    '''
    seed에입력된 사이트에 해당되는 사이트만을 거르기위해 실행됩니다.
    '''
    domain_list = list(url_list.keys())
    full_domain_url_list = []
    for i in domain_list:
        full_domain_url_list.append(url_list[i]['url'])

    return domain_list,full_domain_url_list


def inject_url(domain_list,url_list):
    with open(str(domain_list[0])+'.json', 'w+', encoding="utf-8") as make_file:
        json.dump(url_list, make_file, ensure_ascii=False, indent="\t")
