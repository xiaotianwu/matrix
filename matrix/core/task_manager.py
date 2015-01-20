__author__ = 'xiaotian.wu@chinacache.com'

from matrix.core.logger import logger
from matrix.core.task import *
from matrix.core.task_collection import TaskCollection
from matrix.core.task_distributor import TaskDistributor

class TaskManager:
  def __init__(self, zk_task_trunk = None):
    self.task_collection = TaskCollection(zk_task_trunk)

  def add_list(self, tasks):
    task_ids = []
    for task in tasks:
      task_ids.append(self.task_collection.add(task))
    return task_ids

  def add(self, task):
    return self.task_collection.add(task)

  def remove(self, task_id):
    self.task_collection.remove(task_id)

  def state_transfer(self, task_id, input_action = None):
    self.task_collection.state_transfer(task_id)

  def recover_tasks(self):
    recover_tasks = []
    for task_id in self.task_collection.error_list:
      if task_id not in self.task_collection.task_set:
        logger.error("can not recover error task id: %s, because it's not in task set" % task_id)
        continue
      if TaskProperty.AutoRecover in self.task_collection.task_set[task_id].property:
        recover_tasks.append(task_id)

    for task_id in recover_tasks:
      logger.info("recover task id: %s" % task_id)
      self.state_transfer(task_id, TaskTransferInput.Recover)

  def schedule(self, offers):
    self.recover_tasks()

    pending_tasks = []
    for task_id in self.task_collection.pending_list:
      if task_id not in self.task_collection.task_set:
        continue
      if self.task_collection.task_set[task_id].state != TaskState.Pending:
        logger.error("task id: %s is not pending state" % task_id)
        continue
      self.task_collection.task_set[task_id].clear_offer()
      pending_tasks.append(self.task_collection.task_set[task_id])

    logger.debug('offers: %s' % offers)
    logger.debug('pending tasks: %s' % pending_tasks)
    distributor = TaskDistributor(offers, pending_tasks)
    scheduled_tasks = distributor.assign()
    logger.debug('scheduled tasks: %s' % scheduled_tasks)

    return scheduled_tasks

  def get_task(self, task_id):
    if task_id in self.task_collection.task_set:
      return self.task_collection.task_set[task_id]
    else:
      logger.error("can not get information for task id %s" % task_id)
      return None

if __name__ == '__main__':
  import unittest
  import mesos.interface
  from mesos.interface import mesos_pb2

  class TaskManagerTest(unittest.TestCase):
    def testStateTransfer(self):
      task = Task()
      task.constraint = TaskConstraint()
      task.constraint.cpus = 1
      task.constraint.mem = 1024
      task_manager = TaskManager()
      task_manager.add(task)
      self.assertEqual(task.state, TaskState.Pending)
      self.assertEqual(task.id, 0)
      task_manager.state_transfer(0)
      self.assertEqual(task.state, TaskState.Scheduled)
      task_manager.state_transfer(0)
      self.assertEqual(task.state, TaskState.Running)
      task_manager.state_transfer(0)
      self.assertEqual(task.state, TaskState.Finish)
      task_manager.remove(0)

    def testSchedule(self):
      task1 = Task()
      task1.constraint.cpus = 2
      task1.constraint.mem = 3072
      task1.constraint.host = 'A'
      task2 = Task()
      task2.constraint.cpus = 3
      task2.constraint.mem = 2048
      task3 = Task()
      task3.constraint.cpus = 1
      task3.constraint.mem = 1024
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
      mem_res.scalar.value = 3072
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
      mem_res.scalar.value = 4096
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
      mem_res.scalar.value = 5120
      offers = [offer1, offer2, offer3]
      task_manager = TaskManager()
      task_ids = task_manager.add_list(tasks)
      task_manager.schedule(offers)
      self.assertEqual(task_ids[0], 0)
      self.assertEqual(task_ids[1], 1)
      self.assertEqual(task_ids[2], 2)

  unittest.main()
