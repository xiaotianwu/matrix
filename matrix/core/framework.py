__author__ = 'xiaotian.wu'

import mesos.native
import mesos.interface
from mesos.interface import mesos_pb2
from scheduler import MatrixScheduler
from task import *
import time

class MatrixFramework:
  def __init__(self):
    self._framework = mesos_pb2.FrameworkInfo()
    self._framework.user = ""
    self._framework.id.value = "Matrix"
    self._framework.name = "Matrix"

  def install(self):
    self._scheduler = MatrixScheduler()
    self._driver = mesos.native.MesosSchedulerDriver(
      self._scheduler,
      self._framework,
      "223.202.46.132:5050")
    print "framework install ok"

  def start(self):
    self._driver.run()

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
  time.sleep(100)
  task = Task()
  task.id = 1
  task.name = "test"
  constraint = TaskConstraint()
  constraint.cpus = 1
  constraint.mem = 2048
  task.constraint = constraint
  framework.add_task(task)
