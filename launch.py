import log_manage
from file_manage import *
from crawler import *
from overlap_manage import *

if __name__ == '__main__':
    logger = log_manage.jlog().jongdalogger
    step = logger.info("Depth : ")
    depth = int(input())

    crawl_url_list = get_seed()
    for url in crawl_url_list:
        file_list = []
        set_data = []
        working_count = 0
        url_list, url_file_list = make_dict(url,working_count)
        domain_list, full_domain_url_list = get_domain(url_list)

        clear_url_lsit(domain_list[0])
        inject_url(domain_list,url_list)
        for d in range(depth):
            working_count += 1
            parse_url_list = make_url_list(domain_list,url_list)
            for seed in domain_list:
                completed_url_save(domain=seed, url_list=url_list[seed]['documents'])
            url_parser(parse_url_list,url_list,domain_list,full_domain_url_list,set_data,file_list,working_count)