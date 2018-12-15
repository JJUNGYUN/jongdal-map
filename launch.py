import log_manage
from file_manage import *
from crawler import *
from overlap_manage import *
from selenium_crawler import *

if __name__ == '__main__':
    logger = log_manage.jlog().jongdalogger
    step = logger.info("Depth : ")
    depth = int(input())

    crawl_url_list = get_seed()
    for url in crawl_url_list:
        clear_sub_db()
        file_list = []
        set_data = []
        working_count = 0
        url_list, url_file_list = make_dict(url,working_count)

        clear_url_lsit(list(url_list.keys())[0])
        inject_url(list(url_list.keys())[0], url_list)
        for d in range(depth):
            working_count += 1
            parse_url_list = make_url_list(list(url_list.keys())[0], url_list)
            completed_url_save(domain=list(url_list.keys())[0], url_list=url_list[list(url_list.keys())[0]]['documents'])
            nonscript_crawler(parse_url_list, url_list,working_count)

            script_crawler(url_list, working_count)

            file_manage.fileinfo_save(list(url_list.keys())[0], set_data, file_list)
            file_manage.script_save(list(url_list.keys())[0], url_list[list(url_list.keys())[0]]['url'])
            file_manage.sub_db_to_json(list(url_list.keys())[0], url_list, working_count)


