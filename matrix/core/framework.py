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
    self._framework = mesos_pb2.FrameworkInfo()
    self._framework.user = ""
    self._framework.id.value = ""
    self._framework.failover_timeout = 0
    self._framework.name = "Matrix"

  def install(self):
    self._scheduler = MatrixScheduler()
    host = config.get("mesos", "host")
    if len(host) == 0:
      raise Exception("host is empty")
    logger.info("install framework on mesos host: %s" % host)
    self._driver = mesos.native.MesosSchedulerDriver(
      self._scheduler,
      self._framework,
      host)
    logger.info("framework install ok")

  def start(self):
    self._driver.start()

  def stop(self):
    logger.info("stop framework")
    self._driver.stop()

  def add_task(self, task):
    self._scheduler.add_task(task)

  def remove_task(self, task):
    self._scheduler.remove_task(task)

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
