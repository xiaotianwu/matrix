#!/usr/bin/python

__author__ = 'xiaotian.wu@chinacache.com'

import socket

from flask import Flask
from flask.ext.restful import reqparse, abort, Api, Resource
from kazoo.client import KazooClient

from matrix.core.framework import MatrixFramework
from matrix.core.logger import logger
from matrix.service.api import add, delete, get, health
from matrix.service.flags import parse_flag

flags = parse_flag()
ip = socket.gethostbyname(socket.gethostname())
app = Flask(__name__)
api = Api(app)
parser = reqparse.RequestParser()
parser.add_argument('name', type = str)
parser.add_argument('image', type = str)
parser.add_argument('command', type = str)
parser.add_argument('cpus', type = int)
parser.add_argument('mem', type = int)
parser.add_argument('host', type = str)

zk_client = KazooClient(hosts = flags.zk)
zk_client.start()

framework_name = flags.framework_name
framework_id = flags.framework_id
framework = MatrixFramework(flags.mesos, framework_name, framework_id, zk_client)

class Status(Resource):
  def get(self):
    return health()

api.add_resource(Status, '/matrix/status')

class Get(Resource):
  def get(self, task_id):
    if task_id == "*":
      return "get all task", 200
    else:
      return str(get(framework, int(task_id))), 200

api.add_resource(Get, '/matrix/get/<task_id>')

class Create(Resource):
  def post(self):
    args = parser.parse_args()
    if args['name'] is None or\
       args['image'] is None or\
       args['command'] is None or\
       args['cpus'] is None or\
       args['mem'] is None:
      return "-1", 400
    else:
      task_id = add(framework,
                    args['name'],
                    args['image'],
                    args['command'],
                    args['cpus'],
                    args['mem'],
                    args['host'])
      return str(task_id), 200

api.add_resource(Create, '/matrix/create')

class Delete(Resource):
  def post(self, task_id):
    delete(framework, int(task_id))
    return "1", 200

api.add_resource(Delete, '/matrix/delete/<task_id>')

def i_am_leader():
  logger.info("elected as leader, start leader mode")
  framework.start()
  app.run(host = ip, port = flags.rest_port)

def run():
  election = zk_client.Election(framework_name + "/" + framework_id, ip)
  election.run(i_am_leader)

if __name__ == '__main__':
  run()
