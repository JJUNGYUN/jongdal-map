import logging
import datetime

class jlog:
    def __init__(self):
        self.jongdalogger = logging.getLogger("jongdal")
        self.jongdalogger.setLevel(logging.INFO)

        formtter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        stream_hander = logging.StreamHandler()
        stream_hander.setFormatter(formtter)
        self.jongdalogger.addHandler(stream_hander)

        file_handler = logging.FileHandler("jongdal"+str(datetime.datetime.now().strftime('%Y-%m-%d'))+".log")
        file_handler.setFormatter(formtter)
        self.jongdalogger.addHandler(file_handler)