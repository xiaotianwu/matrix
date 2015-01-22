#!/usr/bin/python

__author__ = 'xiaotian.wu@chinacache.com'

import socket
from SimpleXMLRPCServer import SimpleXMLRPCServer

from kazoo.client import KazooClient

from matrix.core.config import config
from matrix.core.framework import MatrixFramework
from matrix.core.logger import logger
from matrix.core.task import Task, TaskProperty

ip = socket.gethostbyname(socket.gethostname())
port = 30000
zk_client = KazooClient(hosts = "223.202.46.153:2181")
zk_client.start()
framework_name = "MatrixTest"
framework_id = "1"
framework = MatrixFramework("223.202.46.132:5050", framework_name, framework_id, zk_client)

def hello():
  return "hello"

def add(task_name, docker_image, command, cpus, mem, host):
  task = Task()
  task.property.append(TaskProperty.AutoRecover)
  if len(task_name) == 0:
    return False
  if cpus <= 0 or mem <= 0:
    return False
  task.docker_image = docker_image
  task.command = command
  task.name = task_name
  task.constraint.cpus = cpus
  task.constraint.mem = mem
  task.constraint.host = host
  framework.add(task)
  return True

def delete(task_id):
  framework.delete(task_id)
  return True

def get(task_id):
  return framework.get(task_id)

def list_all():
  return True

def start_xml_service():
  logger.info('start matrix web service at host %s:%s' %(ip, port))

  server = SimpleXMLRPCServer((ip, port))
  server.register_introspection_functions()
  server.register_function(hello)
  server.register_function(add)
  server.register_function(delete)
  server.register_function(get)
  server.register_function(list_all)
  server.serve_forever()

def leader():
  logger.info("start leader mode")
  framework.start()
  start_xml_service()

if __name__ == '__main__':
  election = zk_client.Election(framework_name + "/" + framework_id, ip)
  election.run(leader)
