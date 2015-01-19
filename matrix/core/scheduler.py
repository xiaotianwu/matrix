__author__ = 'xiaotian.wu@chinacache.com'

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

from matrix.core.task import *
from matrix.core.task_manager import TaskManager
from matrix.core.logger import logger

class MatrixScheduler(mesos.interface.Scheduler):
  def __init__(self, zk_task_trunk = None):
    self.task_manager = TaskManager(zk_task_trunk)
    self.driver = None
    self.total_task = 10000

  def add(self, task):
    self.task_manager.add(task)

  def get(self, task_id):
    return self.task_manager.get_task(task_id)

  def list(self):
    pass

  def delete(self, task_id):
    self.task_manager.remove(task_id)
    if self.driver is not None:
      self.driver.killTask(task_id)

  def registered(self, driver, frameworkId, masterInfo):
    logger.info("registered framework id %s" % frameworkId.value)
    self.driver = driver

  def resourceOffers(self, driver, offers):
    logger.debug('rescource offers from %s' % [offer.hostname for offer in offers])
    scheduled_tasks = self.task_manager.schedule(offers)

    accept_offer_ids = []
    for task in scheduled_tasks:
      task_info = mesos_pb2.TaskInfo()
      task_info.task_id.value = str(task.id)
      task_info.slave_id.value = task.slave_id
      task_info.name = task.name
      task_info.data = task.command

      docker_info = mesos_pb2.ContainerInfo.DockerInfo()
      docker_info.image = task.docker_image
      docker_info.privileged = True
      docker_info.network = mesos_pb2.ContainerInfo.DockerInfo.HOST
      container_info = mesos_pb2.ContainerInfo()
      container_info.docker.CopyFrom(docker_info)
      container_info.type = mesos_pb2.ContainerInfo.DOCKER
      executor_info = mesos_pb2.ExecutorInfo()
      executor_info.executor_id.value = "MatrixExecutor"
      executor_info.command.value = ""
      executor_info.command.shell = False
      executor_info.name = executor_info.executor_id.value
      executor_info.container.CopyFrom(container_info)

      #executor_info = mesos_pb2.ExecutorInfo()
      #executor_info.executor_id.value = "mesos-executor"
      #executor_info.command.value = "/root/cpdc/mesos_container/mesos_docker_executor/executor.py"
      #executor_info.name = "MesosExecutor"
      #executor_info.source = "MesosPlatform"
     
      task_info.executor.CopyFrom(executor_info)

      cpus = task_info.resources.add()
      cpus.name = "cpus"
      cpus.type = mesos_pb2.Value.SCALAR
      cpus.scalar.value = task.constraint.cpus

      mem = task_info.resources.add()
      mem.name = "mem"
      mem.type = mesos_pb2.Value.SCALAR
      mem.scalar.value = task.constraint.mem

      tasks_info = []
      tasks_info.append(task_info)
      offer_id = mesos_pb2.OfferID()
      offer_id.value = task.offer_id
      driver.launchTasks(offer_id, tasks_info)
      self.task_manager.state_transfer(task.id)
      accept_offer_ids.append(offer_id)

    for offer in offers:
      if offer.id not in accept_offer_ids:
        driver.declineOffer(offer.id)

  def statusUpdate(self, driver, update):
    logger.info('update task id: %s' % update.task_id.value)
    task = self.task_manager.get_task(int(update.task_id.value))
    if task is not None:
      slave_id, executor_id = task.slave_id, task.executor_id
      logger.info('slave id: %s, executor id: %s' % (task.slave_id, task.executor_id))

      if update.state == mesos_pb2.TASK_RUNNING:
        self.task_manager.state_transfer(task.id)
        logger.info("task %s is running" % update.task_id.value)

      if update.state == mesos_pb2.TASK_FINISHED:
        self.task_manager.state_transfer(task.id)
        logger.info("task %s finished, message: %s" % (update.task_id.value, update.message))

      if update.state == mesos_pb2.TASK_FAILED:
        task.clear_offer()
        self.task_manager.state_transfer(task.id, TaskTransferInput.Error)
        logger.error("task %s failed, error str: %s" % (update.task_id.value, update.message))

      if update.state == mesos_pb2.TASK_KILLED or update.state == mesos_pb2.TASK_LOST:
        task.clear_offer()
        self.task_manager.state_transfer(task.id, TaskTransferInput.Error)
        logger.error("task %s killed, state: %s" % (update.task_id.value, update.state))
    else:
      logger.error("task id: %s does not exist" % update.task_id.value)

  def frameworkMessage(self, driver, executorId, slaveId, message):
    pass
