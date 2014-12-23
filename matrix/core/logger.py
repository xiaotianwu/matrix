__author__ = 'xiaotian.wu'

import logging
import logging.config
import os

if not os.path.exists("logging.conf"):
    raise Exception("logging.conf doesn't exist")

logging.config.fileConfig("logging.conf")
logger = logging.getLogger("log")

if __name__ == '__main__':
    logger.info("hello world")
