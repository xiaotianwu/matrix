__author__ = 'xiaotian.wu'

import os
import time

import mesos.native
import mesos.interface
from mesos.interface import mesos_pb2
from scheduler import MatrixScheduler
from task import *

class MatrixFramework:
  def __init__(self):
    self._framework = mesos_pb2.FrameworkInfo()
    self._framework.user = ""
    self._framework.id.value = ""
    self._framework.failover_timeout = 0
    self._framework.name = "Matrix"

  def install(self):
    self._scheduler = MatrixScheduler()
    self._driver = mesos.native.MesosSchedulerDriver(
      self._scheduler,
      self._framework,
      "223.202.46.132:5050")
    print "framework install ok"

  def start(self):
    self._driver.start()

  def stop(self):
    self._driver.stop()

  def add_task(self, task):
    self._scheduler.add_task(task)

  def remove_task(self, task):
    self._scheduler.remove_task(task)

if __name__ == '__main__':
  framework = MatrixFramework()
  framework.install()
  framework.start()
  time.sleep(2)
  print "add task"
  task = Task()
  task.id = 1
  task.name = "test"
  constraint = TaskConstraint()
  constraint.cpus = 1
  constraint.mem = 2048
  constraint.host = "MIS-BJ-6-5C1"
  task.constraint = constraint
  framework.add_task(task)
  time.sleep(60000)
  framework.stop()
