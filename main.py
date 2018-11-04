import log
import sitemap


if __name__ == '__main__':

    logger = log.jlog().jongdalogger
    step = logger.jongdalogger.info("1.Crawl start 2.View crawl site 3.Quit")
    step = int(input())
    if (step == 1):
        depth = int(input('Insert crawl depth : '))
        crawler = sitemap.jongdal(depth=depth,logger=logger)
