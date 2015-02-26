__author__ = 'xiaotian.wu@chinacache.com'

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

from matrix.core.task import *
from matrix.core.manager import TaskManager
from matrix.core.logger import logger

class MatrixScheduler(mesos.interface.Scheduler):
  def __init__(self, task_pickler = None):
    self.task_manager = TaskManager(task_pickler)
    self.driver = None
    self.total_task = 10000
    self.task_number = 0

  def add(self, task):
    return self.task_manager.add(task)

  def get(self, task_id):
    return self.task_manager.get(task_id)

  def list(self):
    raise Exception("not implemented yet")

  def remove(self, task_id):
    if self.driver is not None:
      tid = mesos_pb2.TaskID()
      tid.value = str(task_id)
      logger.error("Matrix driver kill task: %s" % task_id)
      self.driver.killTask(tid)
    else:
      logger.error("Matrix driver not initialized yet")
      return

  def registered(self, driver, frameworkId, masterInfo):
    logger.info("registered framework id %s" % frameworkId.value)
    self.driver = driver

  def create_task_info(self, task):
      task_info = mesos_pb2.TaskInfo()
      task_info.task_id.value = str(task.id)
      task_info.slave_id.value = task.slave_id
      task_info.name = task.name
      task_info.data = str(task.command)

      docker_info = mesos_pb2.ContainerInfo.DockerInfo()
      docker_info.image = task.docker_image
      docker_info.network = mesos_pb2.ContainerInfo.DockerInfo.HOST

      container_info = mesos_pb2.ContainerInfo()
      container_info.docker.CopyFrom(docker_info)
      container_info.type = mesos_pb2.ContainerInfo.DOCKER

      # for container data collection usage
      sys_volume = container_info.volumes.add()
      sys_volume.host_path = "/sys"
      sys_volume.container_path = "/sys"
      sys_volume.mode = mesos_pb2.Volume.RO
      var_lib_docker_volume = container_info.volumes.add()
      var_lib_docker_volume.host_path = "/var/lib/docker"
      var_lib_docker_volume.container_path = "/var/lib/docker"
      var_lib_docker_volume.mode = mesos_pb2.Volume.RO

      for (host_path, container_path) in task.ro_volumes.items():
        ro_volume = container_info.volumes.add()
        ro_volume.host_path = host_path
        ro_volume.container_path = container_path
        ro_volume.mode = mesos_pb2.Volume.RO     

      for (host_path, container_path) in task.rw_volumes.items():
        rw_volume = container_info.volumes.add()
        rw_volume.host_path = host_path
        rw_volume.container_path = container_path
        rw_volume.mode = mesos_pb2.Volume.RW

      executor_info = mesos_pb2.ExecutorInfo()
      executor_info.executor_id.value = ""
      executor_info.command.value = ""
      executor_info.command.shell = False
      executor_info.name = executor_info.executor_id.value
      executor_info.container.CopyFrom(container_info)

      task_info.executor.CopyFrom(executor_info)

      cpus = task_info.resources.add()
      cpus.name = "cpus"
      cpus.type = mesos_pb2.Value.SCALAR
      cpus.scalar.value = task.constraint.cpus

      mem = task_info.resources.add()
      mem.name = "mem"
      mem.type = mesos_pb2.Value.SCALAR
      mem.scalar.value = task.constraint.mem

      return task_info

  def resourceOffers(self, driver, offers):
    logger.debug('rescource offers from %s' % [offer.hostname for offer in offers])
    scheduled_tasks = self.task_manager.schedule(offers)

    accept_offer_ids = []
    offer_id_to_tasks = {}

    # merge tasks with same offer id
    for task in scheduled_tasks:
      if task.offer_id not in offer_id_to_tasks:
        offer_id_to_tasks[task.offer_id] = []
      offer_id_to_tasks[task.offer_id].append(self.create_task_info(task))
      self.task_manager.task_collection.dfa(task.id)
      self.task_number += 1

    for (task_offer_id, task_info_list) in offer_id_to_tasks.items():
      offer_id = mesos_pb2.OfferID()
      offer_id.value = task_offer_id
      driver.launchTasks(offer_id, task_info_list)
      accept_offer_ids.append(offer_id)

    for offer in offers:
      if offer.id not in accept_offer_ids:
        driver.declineOffer(offer.id)

  def statusUpdate(self, driver, update):
    logger.info('update task id: %s' % update.task_id.value)
    task = self.task_manager.get(int(update.task_id.value))
    if task is not None:
      slave_id, executor_id = task.slave_id, task.executor_id
      logger.info('slave id: %s, executor id: %s' % (task.slave_id, task.executor_id))

      if update.state == mesos_pb2.TASK_RUNNING:
        self.task_manager.task_collection.dfa(task.id)
        logger.info("task %s is running" % update.task_id.value)

      if update.state == mesos_pb2.TASK_FINISHED:
        self.task_manager.task_collection.dfa(task.id)
        self.task_manager.remove(task.id)
        logger.info("task %s finished, message: %s" % (update.task_id.value, update.message))

      if update.state == mesos_pb2.TASK_FAILED:
        task.clear_offer()
        self.task_manager.task_collection.dfa(task.id, TaskTransferInput.Error)
        logger.error("task %s failed, error str: %s" % (update.task_id.value, update.message))

      if update.state == mesos_pb2.TASK_KILLED:
        task.clear_offer()
        self.task_manager.task_collection.dfa(task.id, TaskTransferInput.Error)
        logger.error("task %s killed, state: %s" % (update.task_id.value, update.state))

      if update.state == mesos_pb2.TASK_LOST:
        self.task_manager.remove(task.id)
    else:
      logger.error("task id: %s does not exist" % update.task_id.value)

  def frameworkMessage(self, driver, executorId, slaveId, message):
    pass
