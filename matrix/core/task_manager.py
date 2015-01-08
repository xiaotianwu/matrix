__author__ = 'xiaotian.wu'

from collections import deque

from matrix.core.logger import logger
from matrix.core.task import *
from matrix.core.task_distributor import TaskDistributor

class TaskManager:
  def __init__(self):
    self._pending_list = deque()
    self._scheduled_list = set()
    self._running_list = set()
    self._error_list = set()
    self._finish_list = set()
    self._task_set = {}

  def add_list(self, tasks):
    for task in tasks:
      self.add(task)

  def add(self, task):
    if task.id is None:
      raise Exception("task id is none")
    task.state = TaskState.Pending
    self._task_set[task.id] = task
    self._pending_list.append(task.id)

  def remove(self, task_id):
    if task_id in self._task_set:
      state = self._task_set[task_id].state
      if state == TaskState.Pending:
        self._pending_list.remove(task_id)
      elif state == TaskState.Scheduled:
        self._scheduled_list.remove(task_id)
      elif state == TaskState.Running:
        self._running_list.remove(task_id)
      elif state == TaskState.Error:
        self._error_list.remove(task_id)
      elif state == TaskState.Finish:
        self._finish_list.remove(task_id)
      else:
        raise Exception("unknown task state, task id %s %s" % (task_id, state))
      del self._task_set[task_id]

  def move_to_next_state(self, task_id, input_action = None):
    if self._task_set[task_id].state == TaskState.Pending:
      self._pending_list.remove(task_id)
      self._scheduled_list.add(task_id)
      self._task_set[task_id].state = TaskState.Scheduled
    elif self._task_set[task_id].state == TaskState.Scheduled:
      self._scheduled_list.remove(task_id)
      self._running_list.add(task_id)
      self._task_set[task_id].state = TaskState.Running
    elif self._task_set[task_id].state == TaskState.Running:
      self._running_list.remove(task_id)
      if input_action == "error":
        self._error_list.add(task_id)
        self._task_set[task_id].state = TaskState.Error
      else:
        self._finish_list.add(task_id)
        self._task_set[task_id].state = TaskState.Finish
    elif self._task_set[task_id].state == TaskState.Error:
      self._error_list.remove(task_id)
      if input_action == "reschedule":
        self._pending_list.add(task_id)
        self._task_set[task_id].state = TaskState.Pending
      else:
        self._finish_list.add(task_id)
        self._task_set[task_id].state = TaskState.Finish
    else:
      raise Exception("unsupported state")

  def schedule(self, offers):
    pending_tasks = []
    for task_id in self._pending_list:
      if self._task_set[task_id].state != TaskState.Pending:
        raise Exception("task id: %s is not pending state" % task_id)
      self._task_set[task_id].offer = None
      pending_tasks.append(self._task_set[task_id])
    logger.info('offers: %s' % offers)
    logger.info('pending tasks: %s' % pending_tasks)
    distributor = TaskDistributor(offers, pending_tasks)
    scheduled_tasks = distributor.assign()
    logger.info('scheduled tasks: %s' % scheduled_tasks)
    return scheduled_tasks

  def get_task(self, task_id):
    if task_id in self._task_set:
      return self._task_set[task_id]
    else:
      raise Exception("task id: %s does not exist" % task_id)

if __name__ == '__main__':
  import unittest
  import mesos.interface
  from mesos.interface import mesos_pb2

  class TaskManagerTest(unittest.TestCase):
    def testStateTransfer(self):
      task = Task()
      task.id = 1
      task.constraint = TaskConstraint()
      task.constraint.cpus = 1
      task.constraint.mem = 1024
      task_manager = TaskManager()
      task_manager.add(task)
      self.assertEqual(task.state, TaskState.Pending)
      task_manager.move_to_next_state(1)
      self.assertEqual(task.state, TaskState.Scheduled)
      task_manager.move_to_next_state(1)
      self.assertEqual(task.state, TaskState.Running)
      task_manager.move_to_next_state(1)
      self.assertEqual(task.state, TaskState.Finish)
      task_manager.remove(1)

    def testSchedule(self):
      task1 = Task()
      task1.id = '1'
      task1.constraint.cpus = 2
      task1.constraint.mem = 3
      task1.constraint.host = 'A'
      task2 = Task()
      task2.id = '2'
      task2.constraint.cpus = 3
      task2.constraint.mem = 2
      task3 = Task()
      task3.id = '3'
      task3.constraint.cpus = 1
      task3.constraint.mem = 1
      tasks = [task1, task2, task3]
      offer1 = mesos_pb2.Offer()
      offer1.id.value = '1'
      offer1.slave_id.value = '1'
      offer1.hostname = 'A'
      cpus_res = offer1.resources.add()
      cpus_res.name = 'cpus'
      cpus_res.type = mesos_pb2.Value.SCALAR
      cpus_res.scalar.value = 3
      mem_res = offer1.resources.add()
      mem_res.name = 'mem'
      mem_res.type = mesos_pb2.Value.SCALAR
      mem_res.scalar.value = 3
      offer2 = mesos_pb2.Offer()
      offer2.id.value = '2'
      offer2.slave_id.value = '2'
      offer2.hostname = 'B'
      cpus_res = offer2.resources.add()
      cpus_res.name = 'cpus'
      cpus_res.type = mesos_pb2.Value.SCALAR
      cpus_res.scalar.value = 4
      mem_res = offer2.resources.add()
      mem_res.name = 'mem'
      mem_res.type = mesos_pb2.Value.SCALAR
      mem_res.scalar.value = 4
      offer3 = mesos_pb2.Offer()
      offer3.id.value = '3'
      offer3.slave_id.value = '3'
      offer3.hostname = 'C'
      cpus_res = offer3.resources.add()
      cpus_res.name = 'cpus'
      cpus_res.type = mesos_pb2.Value.SCALAR
      cpus_res.scalar.value = 5
      mem_res = offer3.resources.add()
      mem_res.name = 'mem'
      mem_res.type = mesos_pb2.Value.SCALAR
      mem_res.scalar.value = 5
      offers = [offer1, offer2, offer3]
      task_manager = TaskManager()
      task_manager.add_list(tasks)
      task_manager.schedule(offers)

  unittest.main()
