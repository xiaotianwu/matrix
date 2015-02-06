__author__ = 'xiaotian.wu@chinacache.com'

import os
import ConfigParser

matrix_conf = "/matrix/matrix/core/matrix.conf"

if not os.path.exists(matrix_conf):
    raise Exception("matrix.conf doesn't exist")

config = ConfigParser.ConfigParser()
config.read(matrix_conf)

if __name__ == '__main__':
    print config.get("mesos", "host")
