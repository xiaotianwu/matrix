#!/usr/bin/python

__author__ = 'xiaotian.wu@chinacache.com'

import mesos.native
import mesos.interface
from mesos.interface import mesos_pb2

from matrix.core.scheduler import MatrixScheduler
from matrix.core.task import *
from matrix.core.logger import logger
from matrix.core.pickler import TaskPickler

class MatrixFramework:
  def __init__(self, mesos_host, framework_name, framework_id, zk_client = None):
    self.framework = mesos_pb2.FrameworkInfo()
    self.framework.user = ""
    self.framework.id.value = ""
    self.framework.failover_timeout = 0
    self.framework.name = framework_name
    self.mesos_host = mesos_host
    self.zk_client = zk_client
    if len(mesos_host) == 0:
      raise Exception("host not filled")

  def start(self):
    self.task_pickler = TaskPickler(self.framework.name, "1", self.zk_client)
    self.scheduler = MatrixScheduler(self.task_pickler)
    logger.info("start framework")
    self.driver = mesos.native.MesosSchedulerDriver(
      self.scheduler,
      self.framework,
      self.mesos_host)
    self.driver.start()

  def stop(self):
    logger.info("stop framework")
    self.driver.stop()

  def add(self, task):
    return self.scheduler.add(task)

  def get(self, task_id):
    return self.scheduler.get(task_id)

  def list(self, condition):
    raise Exception("not implemented yet")

  def delete(self, task_id):
    self.scheduler.delete(task_id)

if __name__ == '__main__':
  import time
  from kazoo.client import KazooClient

  zk_client = KazooClient(hosts = "223.202.46.153:2181")
  zk_client.start()
  framework = MatrixFramework("223.202.46.132:5050", "MatrixTest", "1", zk_client)
  framework.start()
  time.sleep(2)
  task = Task()
  task.property.append(TaskProperty.AutoRecover)
  task.docker_image = 'cpdc/walle_monitor'
  task.command = '/walle_monitor_0_2_1/monitor/main.py --kafka_hosts=223.203.199.153:9092,223.203.199.152:9092,223.203.199.150:9092,223.203.199.149:9092,223.203.199.148:9092 --topic=final-log --ip_mapping_file=/walle_monitor_0_2_1/monitor/ipmapping.conf'
  task.name = "test"
  task.constraint.cpus = 1
  task.constraint.mem = 2048
  task.constraint.host = "MIS-BJ-6-5A2"
  framework.add(task)
  time.sleep(6000000)
  framework.stop()
