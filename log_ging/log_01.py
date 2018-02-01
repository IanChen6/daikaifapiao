# -*- coding:utf-8 -*-
__author__ = 'IanChen'

import logging

# logging.basicConfig(filename="test.log",level=logging.INFO)
#
# logger=logging.getLogger("test.log")
# logger.setLevel(logging.ERROR)
# logging.warning("bitch")
# logging.info("hha")
# logging.error("see")
# logging.critical("hello")
def create_logger(level=logging.DEBUG,path="task"):
# create logger
    logger_name = "example"
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)

# create file handler
    log_path = './logs/{}log.log'.format(path)
    fh = logging.FileHandler(log_path,encoding='utf8')
    fh.setLevel(level)

# CREATE FORMATTER
    fmt = "%(asctime)s %(levelname)s %(filename)s %(lineno)d %(thread)d %(process)d %(message)s"
    datefmt = "%a %d %b %Y %H:%M:%S"

    formatter = logging.Formatter(fmt, datefmt)

# add handler and formatter to logger
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger
# print log info
if __name__=="__main__":
    logger=create_logger(logging.ERROR)
    logger.debug('debug message')
    logger.info("info")
    logger.warning("warn")
    logger.error("error")
    logger.critical('critical')