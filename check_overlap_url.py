import pickle

def completed_url_save(domain,url_list):
    with open('checked_pickle/'+str(domain), 'ab') as f:
        for i in url_list:
            pickle.dump(i, f)

def check_overlap_url(domain,url_list):
    completed_url_list = []
    ncompleted_url_list = []
    try :
        with open('checked_pickle/'+str(domain), 'rb') as f:
            while 1:
                try:
                    u = pickle.load(f)
                    completed_url_list.append(u)
                except EOFError:
                    break
        for url in url_list:
            if url not in completed_url_list:
                ncompleted_url_list.append(url)
    except FileNotFoundError as e:
        f = open('checked_pickle/'+str(domain), 'wb+')
        f.close()

        with open('checked_pickle/'+str(domain), 'rb') as f:
            while 1:
                try:
                    u = pickle.load(f)
                    completed_url_list.append(u)
                except EOFError:
                    break
        for url in url_list:
            if url not in completed_url_list:
                ncompleted_url_list.append(url)

    return ncompleted_url_list