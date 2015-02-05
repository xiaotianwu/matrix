__author__ = 'xiaotian.wu@chinacache.com'

from flask import Flask
from flask import request
from kazoo.client import KazooClient

from matrix.core.config import config
from matrix.core.framework import MatrixFramework
from matrix.core.logger import logger
from matrix.service.api import add, delete, get

app = Flask(__name__)
ip = socket.gethostbyname(socket.gethostname())
port = 30000

zk_client = KazooClient(hosts = "223.202.46.153:2181")
zk_client.start()

framework_name = "Matrix"
framework_id = "1"
framework = MatrixFramework("223.202.46.132:5050", framework_name, framework_id, zk_client)

@app.route('/add')
def add_task():
  return add()

@app.route('/delete')
def delete_task():
  return delete()

@app.route('/get')
def get_task():
  return get()

def run():
  election = zk_client.Election(framework_name + "/" + framework_id, ip)
  election.run(i_am_leader)

def i_am_leader():
  logger.info("i have been elected as leader, start leader mode")
  framework.start()
  app.run(host = ip)
