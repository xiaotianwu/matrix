__author__ = 'xiaotian.wu'

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

from task_manager import TaskManager

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
      task_info.executor = mesos_pb2.ExecutorInfo()
      task_info.executor.executor_id.value = "matrix-executor"
      task_info.executor.command.value = ""
      task_info.executor.name = "MatrixExecutor"

      cpus = task_info.resources.add()
      cpus.name = "cpus"
      cpus.type = mesos_pb2.Value.SCALAR
      cpus.scalar.value = task.constraint.cpus

      mem = task_info.resources.add()
      mem.name = "mem"
      mem.type = mesos_pb2.Value.SCALAR
      mem.scalar.value = task.constraint.mem

      task_info.slave_id = task.offer.slave_id
      task_info.executor_id = task.executor.executor_id
      driver.launchTasks(task.offer.id, [task_info])
      self._task_manager.move_to_next_state(task.id)
      accept_offer_ids.add(task.offer.id)

    for offer in offers:
      if offer.id not in accept_offer_ids:
        driver.declineOffer(offer.id)

  def statusUpdate(self, driver, update):
    task = self._task_manager.get_task(update.task_id.value)
    slave_id, executor_id = task.slave_id, task.executor_id

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
