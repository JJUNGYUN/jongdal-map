from bs4 import BeautifulSoup
from selenium import webdriver
from multiprocessing import Queue, Process, Manager
import log_manage
import file_manage
from crawler import contact, data_parser, process_starter


def selenium_option():
    options = webdriver.ChromeOptions()
    options.add_argument('headless')
    options.add_argument('window-size=1920x1080')
    options.add_argument("disable-gpu")
    options.add_argument('User-Agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) \
        Chrome/60.0.3112.113 Safari/537.36')
    options.add_argument('Accept=text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8')
    options.add_argument('Accept-Charset=ISO-8859-1,utf-8;q=0.7,*;q=0.3')
    options.add_argument('Accept-Encoding=none')
    options.add_argument('Accept-Language=en-US,en;q=0.8')
    options.add_argument('Connection=keep-alive')

    return options


def selenium_crawler(url,scripts):

    driver = webdriver.Chrome('./chromedriver.exe',options=selenium_option())
    driver.implicitly_wait(3)

    parsing_url_html = []
    parsing_url = []

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




def script_crawl(url, script,parsed_url_list, file_url_list, script_list, url_list,working_count,q):
    scripts = dict()
    logger = log_manage.jlog()
    logger.info(" Working Selenium depth : " + str(working_count) + str(" | start parsing url : " + url))
    try:
        parsing_url, parsing_url_html = selenium_crawler(url, scripts=script) # 셀레니움 그리드를 사용
        for cnt in range(len(parsing_url_html)):
            scripts[parsing_url[cnt]] = list()
            for i in parsing_url_html[cnt]:
                try:
                    if i['href'][0] == '/' and (list(url_list.keys())[0] in parsing_url[cnt]):
                        if not contact(i['href']):
                            file_url_list.append(
                                data_parser(url_list[list(url_list.keys())[0]]['url'] + i['href'], parsing_url[cnt],
                                            list(url_list.keys())[0]))
                        parsed_url_list.append(url_list[list(url_list.keys())[0]]['url'] + i['href'][1:])
                    elif (list(url_list.keys())[0] in i['href']) and (
                            i not in url_list[list(url_list.keys())[0]]['documents']):
                        if not contact(i['href']):
                            file_url_list.append(data_parser(i['href'], parsing_url[cnt], list(url_list.keys())[0]))
                        parsed_url_list.append(i['href'])
                    elif 'http' not in i['href']:
                        scripts[parsing_url[cnt]].append(i['href'])
                except Exception as e:
                    continue
    except Exception as e:
        logger.error(str(e))
        return False

    logger.info(" Working Selenium depth : " + str(working_count) + str(" | end parsing url : " + url))
    q.get()
    script_list.append(scripts)

def script_crawler(url_list,working_count):
    '''
    들어간 url을 bs4를 이용하여 html을 파싱합니다
    :return:
    '''
    q = Queue()
    processes = []
    #file_manage.clear_sub_db()
    urls, url_scripts = file_manage.load_script_urls()
    manager = Manager()
    # with Manager() as manager:
    parsed_url_list = Manager().list()
    file_url_list = Manager().list()
    script_list = Manager().list()
    print(urls,url_scripts)
    for cnt in range(len(urls)):
        if q.qsize() > 10:
            processes = process_starter(processes)
            while True:
                for i in processes:
                    if not i.is_alive():
                        processes.remove(i)
                if len(processes) == 0:
                    break
        if len(parsed_url_list) > 1000:
            file_manage.save_sub_db(parsed_url_list)
            parsed_url_list = Manager().list()
        # script_crawl(urls[cnt], url_scripts[cnt], parsed_url_list,
                                                   # file_url_list, script_list,  url_list,working_count)

        p = Process(target=script_crawl, args=(urls[cnt], url_scripts[cnt], parsed_url_list,
                                                    file_url_list, script_list,  url_list,working_count,q))
        q.put(urls[cnt])
        processes.append(p)


    print(processes)
    process_starter(processes)

    while True:
        for i in processes:
            if not i.is_alive():
                processes.remove(i)
        if len(processes) == 0:
            break

    file_manage.save_sub_db(parsed_url_list)
    file_manage.save_script_db(script_list)
    file_manage.save_filesub_db(file_url_list)