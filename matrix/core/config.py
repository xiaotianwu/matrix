__author__ = 'xiaotian.wu@chinacache.com'

import os
import ConfigParser

if not os.path.exists("matrix.conf"):
    raise Exception("matrix.conf doesn't exist")

config = ConfigParser.ConfigParser()
config.read("matrix.conf")

if __name__ == '__main__':
    print config.get("mesos", "host")
