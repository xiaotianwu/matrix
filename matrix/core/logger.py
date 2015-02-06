__author__ = 'xiaotian.wu'

import logging
import logging.config
import os

logging_conf = "/matrix/matrix/core/logging.conf"

if not os.path.exists(logging_conf):
    raise Exception("logging.conf doesn't exist")

logging.config.fileConfig(logging_conf)
logger = logging.getLogger("matrix")

if __name__ == '__main__':
    logger.info("hello world")
