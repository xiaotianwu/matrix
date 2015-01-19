#!/usr/bin/python

__author__ = 'xiaotian.wu@chinacache.com'

import socket
from SimpleXMLRPCServer import SimpleXMLRPCServer

from matrix.core.config import config
from matrix.core.framework import MatrixFramework
from matrix.core.logger import logger
from matrix.core.task import Task, TaskProperty

framework = MatrixFramework("MatrixTest", "1", "223.202.46.153:2181")
framework.install()
framework.start()

def hello():
  return "hello"

def add(task_id, task_name, docker_image, command, cpus, mem, host):
  task = Task()
  task.property.append(TaskProperty.AutoRecover)
  task.id = task_id
  task.docker_image = docker_image
  task.command = command
  task.name = task_name
  task.constraint.cpus = cpus
  task.constraint.mem = mem
  task.constraint.host = host
  print task.id
  print task.docker_image
  print task.command
  print task.name
  print task.command
  print task.constraint.cpus
  print task.constraint.mem
  print task.constraint.host
  framework.add(task)
  return True

def delete(task_id):
  return True

def get(task_id):
  return True

def list_all():
  return True

if __name__ == '__main__':
  ip = socket.gethostbyname(socket.gethostname())
  port = 30000
  logger.info('start server at host %s:%s' %(ip, port))

  server = SimpleXMLRPCServer((ip, port))
  server.register_introspection_functions()
  server.register_function(hello)
  server.register_function(add)
  server.register_function(delete)
  server.register_function(get)
  server.register_function(list_all)
  server.serve_forever()
