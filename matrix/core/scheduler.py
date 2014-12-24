__author__ = 'xiaotian.wu'

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

from task_manager import TaskManager
from logger import logger

class MatrixScheduler(mesos.interface.Scheduler):
  total_task = 10000

  def __init__(self):
    self._task_manager = TaskManager()

  def add_task(self, task):
    self._task_manager.add(task)

  def remove_task(self, task_id):
    self._task_manager.remove(task_id)

  def registered(self, driver, frameworkId, masterInfo):
    logger.info("registered framework id %s" % frameworkId.value)
    self._driver = driver

  def resourceOffers(self, driver, offers):
    logger.info('rescource offers from %s' % [offer.hostname for offer in offers])
    scheduled_tasks = self._task_manager.schedule(offers)

    accept_offer_ids = []
    for task in scheduled_tasks:
      task_info = mesos_pb2.TaskInfo()
      task_info.task_id.value = str(task.id)
      task_info.slave_id.value = task.offer.slave_id.value
      task_info.name = task.name

      docker_info = mesos_pb2.ContainerInfo.DockerInfo()
      docker_info.image = "xxxx/xxxx"
      docker_info.network = mesos_pb2.ContainerInfo.DockerInfo.HOST
      container_info = mesos_pb2.ContainerInfo()
      container_info.docker.CopyFrom(docker_info)
      container_info.type = mesos_pb2.ContainerInfo.DOCKER
      executor_info = mesos_pb2.ExecutorInfo()
      executor_info.container.CopyFrom(container_info)
     
      task_info.executor.CopyFrom(executor_info)
      task_info.executor.executor_id.value = "matrix-executor"
      task_info.executor.command.value = "yyyy"
      task_info.executor.name = "MatrixExecutor"

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
      driver.launchTasks(task.offer.id, tasks_info)
      self._task_manager.move_to_next_state(task.id)
      accept_offer_ids.append(task.offer.id)

    for offer in offers:
      if offer.id not in accept_offer_ids:
        driver.declineOffer(offer.id)

  def statusUpdate(self, driver, update):
    print 'update task id:', update.task_id.value
    task = self._task_manager.get_task(int(update.task_id.value))
    slave_id, executor_id = task.slave_id, task.executor_id
    print 'slave id: %s, executor id: %s' % (task.slave_id, task.executor_id)

    if update.state == mesos_pb2.TASK_RUNNING:
      logger.info("task %s is running" % update.task_id.value)
      self._task_manager.move_to_next_state(task.id)

    if update.state == mesos_pb2.TASK_FINISHED:
      logger.info("task %s finished" % update.task_id.value)
      self._task_manager.move_to_next_state(task.id)

    if update.state == mesos_pb2.TASK_FAILED:
      self._task_manager.move_to_next_state(task.id, "error")
      logger.error("task %s failed, error str: %s" % (update.task_id.value, update.data))

    if update.state == mesos_pb2.TASK_KILLED or\
      update.state == mesos_pb2.TASK_LOST:
      logger.error("task %s killed, state: %s" % (update.task_id.value, update.state))

  def frameworkMessage(self, driver, executorId, slaveId, message):
    pass
