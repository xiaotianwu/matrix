#!/usr/bin/python

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
from matrix.core.zookeeper_task_trunk import ZookeeperTaskTrunk

class MatrixFramework:
  def __init__(self, framework_name, framework_id, zk_hosts = None):
    if zk_hosts is not None:
      self.zk_task_trunk = ZookeeperTaskTrunk(framework_name, framework_id, zk_hosts)
    else:
      self.zk_task_trunk = None
    self.framework = mesos_pb2.FrameworkInfo()
    self.framework.user = ""
    self.framework.id.value = ""
    self.framework.failover_timeout = 0
    self.framework.name = "Matrix"

  def install(self):
    self.scheduler = MatrixScheduler(self.zk_task_trunk)
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

  def add(self, task):
    self.scheduler.add(task)

  def get(self, task_id):
    pass

  def list(self, condition):
    pass

  def delete(self, task_id):
    self.scheduler.delete(task_id)

if __name__ == '__main__':
  framework = MatrixFramework("MatrixTest", "1", "223.202.46.153:2181")
  framework.install()
  framework.start()
  time.sleep(5)
  print "add task"
  task = Task()
  task.property.append(TaskProperty.AutoRecover)
  task.id = 1
  task.docker_image = 'cpdc/walle_monitor'
  #task.docker_image = 'cpdc/mesos_executor_0_2_2'
  task.command = '/walle_monitor_0_2_1/monitor/main.py --kafka_hosts=223.203.199.153:9092,223.203.199.152:9092,223.203.199.150:9092,223.203.199.149:9092,223.203.199.148:9092 --topic=final-log --ip_mapping_file=/walle_monitor_0_2_1/monitor/ipmapping.conf'
  #task.command = 'ls -l'
  task.name = "test"
  task.constraint.cpus = 1
  task.constraint.mem = 2048
  task.constraint.host = "MIS-BJ-6-5A2"
  framework.add(task)
  time.sleep(6000000)
  framework.stop()
