import pickle
from crawler import *


def save_sub_db(l):
    with open('sub_db.pickle', 'ab') as f:
        for a in l:
            pickle.dump(a, f)


def save_filesub_db(l):
    with open('file_sub_db.pickle', 'ab') as f:
        for a in l:
            pickle.dump(a, f)


def clear_sub_db():
    with open('sub_db.pickle', 'wb+') as f:
        pickle.dump("\n",f)


def fileinfo_save(domain_list,set_data,file_list):
    f = open('file_sub_db.pickle', 'rb')
    url_file_list = []
    while 1:
        try:
            u = pickle.load(f)
            url_file_list.append(u)
        except EOFError:
            break

    with open(str(domain_list[0]) + '_file.json', 'w+', encoding="utf-8") as f:
        json.dump(set_save_data(url_file_list,set_data,file_list),f,ensure_ascii=False, indent="\t")


def set_save_data(data,set_data,file_list):
    for i in data:
        if i["fileUrl"] not in file_list:
            file_list.append(i["fileUrl"])
            set_data.append(i)

    return set_data


def sub_db_to_json(domain_list,url_list,working_count):
    f = open('sub_db.pickle', 'rb')
    url_ = []
    while 1:
        try:
            u = pickle.load(f)
            url_.append(u)
        except EOFError:
            break

    for i in url_:
        for seed in domain_list:
            if (seed in i):
                url_list[seed]['documents'].append(i)
    for seed in domain_list:
        url_list[seed]['documents'] = list(set(url_list[seed]['documents']))
        url_list[seed]['depth'] = working_count
        url_list[seed]['end_date'] = str(datetime.datetime.now())
    inject_url(domain_list,url_list)


def get_seed():
    '''
    seed.txt에 입력된 사이트들을 가져옵니다.
    '''
    f = open('seed.txt','r')
    return f
