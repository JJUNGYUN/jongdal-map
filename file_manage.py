import pickle
from crawler import *
import os


def save_sub_db(l):
    with open('sub_db.pickle', 'ab') as f:
        for a in l:
            pickle.dump(a, f)


def save_script_db(l):

    with open('script_sub_db.pickle', 'ab') as f:
        for a in l:
            pickle.dump(a, f)


def load_script_urls():
    try:
        with open("./dcinside.com_script.json", "r", encoding="utf8") as f:
            data = json.load(f)
    except:
        return [],[]
    urls = list(dict(data).keys())

    url_scripts = list(dict(data).values())

    return urls, url_scripts


def save_filesub_db(l):
    with open('file_sub_db.pickle', 'ab') as f:
        for a in l:
            pickle.dump(a, f)


def clear_sub_db():
    try:
        os.remove('./script_sub_db.pickle')
        os.remove('./file_sub_db.pickle')
        os.remove('./sub_db.pickle')
    except:
        pass


def fileinfo_save(domain,set_data,file_list):
    f = open('file_sub_db.pickle', 'rb')
    url_file_list = []
    while 1:
        try:
            u = pickle.load(f)
            url_file_list.append(u)
        except EOFError:
            break
    with open(str(domain) + '_file.json', 'w+', encoding="utf-8") as f:
        json.dump(set_save_data(url_file_list,set_data,file_list),f,ensure_ascii=False, indent="\t")


def script_save(domain):
    f = open('script_sub_db.pickle', 'rb')
    save_data = dict()
    while 1:
        try:
            u = pickle.load(f)
            if len(dict(u)[list(dict(u).keys())[0]]) == 0:
                continue
            save_data[list(dict(u).keys())[0]] = list(set(list(dict(u)[list(dict(u).keys())[0]])))
        except EOFError:
            break

    with open(str(domain) + '_script.json', 'w+', encoding="utf-8") as f:
        json.dump(save_data,f,ensure_ascii=False, indent="\t")


def set_save_data(data,set_data,file_list):
    for i in data:
        if i["fileUrl"] not in file_list:
            file_list.append(i["fileUrl"])
            set_data.append(i)

    return set_data


def sub_db_to_json(domain,url_list,working_count):
    f = open('sub_db.pickle', 'rb')
    url_ = []
    while 1:
        try:
            u = pickle.load(f)
            url_.append(u)
        except EOFError:
            break

    for i in url_:
        if (domain in i):
            url_list[domain]['documents'].append(i)
    url_list[domain]['documents'] = list(set(url_list[domain]['documents']))
    url_list[domain]['depth'] = working_count
    url_list[domain]['end_date'] = str(datetime.datetime.now())
    inject_url(domain,url_list)


def get_seed():
    '''
    seed.txt에 입력된 사이트들을 가져옵니다.
    '''
    f = open('seed.txt','r')
    return f
