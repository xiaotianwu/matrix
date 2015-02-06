__author__ = 'xiaotian.wu@chinacache.com'

import socket

from flask import Flask
from flask import request
from kazoo.client import KazooClient

from matrix.core.config import config
from matrix.core.framework import MatrixFramework
from matrix.core.logger import logger
from matrix.service.api import add, delete, get
from matrix.service.flags import parse_flag

flags = parse_flag()
ip = socket.gethostbyname(socket.gethostname())
app = Flask(__name__)
port = flags.rest_port

zk_client = KazooClient(hosts = flags.zk)
zk_client.start()

framework_name = flags.framework_name
framework_id = flags.framework_id
framework = MatrixFramework(flags.mesos, framework_name, framework_id, zk_client)

@app.route('/add', methods = ['POST'])
def add_task():
  return add()

@app.route('/delete', methods = ['POST'])
def delete_task():
  return delete()

@app.route('/get', methods = ['POST'])
def get_task():
  return get()

def run():
  election = zk_client.Election(framework_name + "/" + framework_id, ip)
  election.run(i_am_leader)

def i_am_leader():
  logger.info("elected as leader, start leader mode")
  framework.start()
  app.run(host = ip, port = port)

if __name__ == '__main__':
  run()
