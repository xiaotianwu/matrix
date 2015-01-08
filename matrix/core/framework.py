__author__ = 'xiaotian.wu@chinacache.com'

import os
import time

import mesos.native
import mesos.interface
from mesos.interface import mesos_pb2
from matrix.core.scheduler import MatrixScheduler
from matrix.core.task import *
from matrix.core.config import config
from matrix.core.logger import logger

class MatrixFramework:
  def __init__(self):
    self.framework = mesos_pb2.FrameworkInfo()
    self.framework.user = ""
    self.framework.id.value = ""
    self.framework.failover_timeout = 0
    self.framework.name = "Matrix"

  def install(self):
    self.scheduler = MatrixScheduler()
    host = config.get("mesos", "host")
    if len(host) == 0:
      raise Exception("host is empty")
    logger.info("install framework on mesos host: %s" % host)
    self.driver = mesos.native.MesosSchedulerDriver(
      self.scheduler,
      self.framework,
      host)
    logger.info("framework install ok")

  def start(self):
    self.driver.start()

  def stop(self):
    logger.info("stop framework")
    self.driver.stop()

  def new(self, task):
    self.scheduler.new(task)

  def get(self, task_id):
    pass

  def list(self, condition):
    pass

  def delete(self, task_id):
    self.scheduler.delete(task_id)

if __name__ == '__main__':
  framework = MatrixFramework()
  framework.install()
  framework.start()
  time.sleep(20)
  #print "add task"
  #task = Task()
  #task.id = 1
  #task.name = "test"
  #constraint = TaskConstraint()
  #constraint.cpus = 1
  #constraint.mem = 2048
  #constraint.host = "MIS-BJ-6-5A2"
  #task.constraint = constraint
  #framework.add_task(task)
  #time.sleep(60000)
  framework.stop()
