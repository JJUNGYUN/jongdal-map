from bs4 import BeautifulSoup
import urllib.request
import urllib.parse
import pandas as pd
import json
import re
import datetime
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from multiprocessing import Queue, Process, Manager
import pickle
import log_manage
import file_manage
import overlap_manage


def make_url_list(domain,url_list):
    '''
    크롤링할 url의 리스트를 제작합니다.
    '''
    parse_url_list = []

    ncompleted_list = overlap_manage.check_overlap_url(domain=domain, url_list=url_list[str(domain)]['documents'])
    for url in ncompleted_list:
        parse_url_list.append(url)

    return parse_url_list


def process_starter(processes):
    for p in processes:
        p.start()

    for p in processes:
        p.join(6)

    return []

def get_html_sel(url,scripts):
    parsing_url_html = []
    parsing_url = []
    driver = webdriver.Remote(command_executor='http://192.168.122.129:4444/wd/hub',
                              desired_capabilities={'browserName': 'chrome'})

    for script in scripts:
        driver.get(url)
        driver.implicitly_wait(5)
        try:
            driver.execute_script(script)
            driver.implicitly_wait(5)
            html = driver.page_source
            soup = BeautifulSoup(html, 'html.parser')
            parsing_url.append(driver.current_url)
            parsing_url_html.append(soup.find_all())
        except Exception as e:
            print(e)

    driver.quit()

    return parsing_url, parsing_url_html


def load_script_urls():
    try:
        with open("./dcinside.com_script.json", "r", encoding="utf8") as f:
            data = json.load(f)
    except:
        return [],[]
    urls = list(dict(data).keys())

    url_scripts = list(dict(data).values())

    return urls, url_scripts


def nonscript_crawler(parse_url_list,url_list,working_count):
    '''
    들어간 url을 bs4를 이용하여 html을 파싱합니다
    :return:
    '''
    q = Queue()
    processes = []
    file_manage.clear_sub_db()
    with Manager() as manager:
        parsed_url_list = manager.list()
        file_url_list = manager.list()
        script_list = manager.list()
        for parse_url in parse_url_list:
            if q.qsize() > 30:
                processes= process_starter(processes)
            if len(parsed_url_list) > 1000:
                file_manage.save_sub_db(parsed_url_list)
                parsed_url_list = manager.list()

            p = Process(target=parsing_url, args=(parse_url, parsed_url_list, file_url_list, url_list,
                                                  q, working_count,script_list))
            q.put(parse_url)
            processes.append(p)

        processes = process_starter(processes)
        file_manage.save_sub_db(parsed_url_list)
        file_manage.save_script_db(script_list)
        file_manage.save_filesub_db(file_url_list)




def connect_url(url):
    logger = log_manage.jlog()
    hdr = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)\
         Chrome/60.0.3112.113 Safari/537.36',
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


def data_parser(fileurl, introurl, domain):
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


def contact(url):
    file_list = ['.PDF' ,'.MP4' ,'.DOC' ,'.docx' ,'.pdf' ,'.jpg','.bmp','.jpeg','.mp4','.doc','.exe','.pptx'
         ,'.png','.mp3','.doc','.docx','.ppt','.zip','.tar.gz','.rar','.alz','.az','.7zip','.tar',
         '.iso','.wmf','.WMF','.csv','.xls','.GIF','.gif','.exe']
    for i in file_list:
        if i in url:
            return False

    return True


def parsing_url(parse_url, parsed_url_list, file_url_list, url_list,q, working_count,script_list):
    '''
    전달받은 html문서에서 seed.txt에 입력된 사이트에 해당이 되는 url을 파싱하여 저장합니다.
    '''
    logger = log_manage.jlog()
    script = dict()
    script[parse_url] = list()
    if contact(parse_url):
        logger.info(" Working depth : "+str(working_count)+str(" | start parsing url : "+parse_url))
        try:
            for i in connect_url(parse_url):
                try:
                    if i['href'][0] == '/' and (list(url_list.keys())[0] in parse_url):
                        if not contact(i['href']):
                            file_url_list.append(data_parser(url_list[list(url_list.keys())[0]]['url']+i['href'],parse_url, list(url_list.keys())[0]))
                        parsed_url_list.append(url_list[list(url_list.keys())[0]]['url']+i['href'][1:])
                    elif (list(url_list.keys())[0] in i['href']) and (i not in url_list[list(url_list.keys())[0]]['documents']):
                        if not contact(i['href']):
                            file_url_list.append(data_parser(i['href'], parse_url, list(url_list.keys())[0]))
                        parsed_url_list.append(i['href'])
                    elif ('http' not in i['href']) and href_abc(i['href']):
                        parsed_url_list.append(url_list[list(url_list.keys())[0]]['url'] + "/" + i['href'][1:])
                    elif 'http' not in i['href'] :
                        script[parse_url].append(i['href'])
                except Exception as e:
                    continue
            logger.info(" Working depth : " + str(working_count) + str(" | end parsing url : " + parse_url))
        except Exception as e:
            logger.error(str(e))

        q.get()
    else:
        logger.info(str(parse_url)+"is file url")
    script_list.append(script)


def href_abc(url):
    for i in range(97, 123):
        if url[0] == i:
            return True

    return False


def make_dict(url,working_count):
    '''
    "json에 저장하기위한 dict형식의 변수를 생성합니다."
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


def inject_url(domain,url_list):
    with open(str(domain)+'.json', 'w+', encoding="utf-8") as make_file:
        json.dump(url_list, make_file, ensure_ascii=False, indent="\t")
