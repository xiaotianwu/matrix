__author__ = 'xiaotian.wu@chinacache.com'

from collections import deque

from matrix.core.logger import logger
from matrix.core.task import (TaskState, TaskConstraint,
                              TaskTransferInput, Task,
                              serialize_task, deserialize_task)
 
class TaskCollection:
  def __init__(self, task_pickler = None):
    self.pending_list = deque()
    self.scheduled_list = set()
    self.running_list = set()
    self.error_list = set()
    self.finish_list = set()
    self.task_set = {}
    self.task_pickler = task_pickler
    self.next_task_id = 0

  def init_from_zookeeper(self):
    if self.task_pickler is None:
      raise Exception("zookeeper instance not exists")

    logger.info("init task info from zookeeper") 
    all_task_data = self.task_pickler.get_all_task()
    all_task = [deserialize_task(str(data)) for data in all_task_data]
    max_task_id = -1

    for task in all_task:
      self.task_set[task.id] = task
      if task.id > max_task_id:
        max_task_id = task.id
      if task.state == TaskState.Pending:
        self.pending_list.append(task.id)
      elif task.state == TaskState.Scheduled:
        self.scheduled_list.add(task.id)
      elif task.state == TaskState.Running:
        self.running_list.add(task.id)
      elif task.state == TaskState.Error:
        self.error_list.add(task.id)
      elif task.state == TaskState.Finish:
        self.finish_list.add(task.id)
      else:
        logger.error("task id: %s, unknown task state: %s" % (task.id, task.state))
    self.next_task_id = max_task_id + 1
    logger.info("init task info from zookeeper success")

    if logger.isEnabledFor(logging.DEBUG):
      logger.debug("----------------pending list-----------------")
      for task_id in self.pending_list:
        logger.debug(str(self.task_set[task_id]))
      logger.debug("--------------scheduled list-----------------")
      for task_id in self.scheduled_list:
        logger.debug(str(self.task_set[task_id]))
      logger.debug("----------------running list-----------------")
      for task_id in self.running_list:
        logger.debug(str(self.task_set[task_id]))
      logger.debug("------------------error list-----------------")
      for task_id in self.error_list:
        logger.debug(str(self.task_set[task_id]))
      logger.debug("-----------------finish list-----------------")
      for task_id in self.finish_list:
        logger.debug(str(self.task_set[task_id]))

  def add(self, task):
    task.id = self.next_task_id
    self.next_task_id += 1

    task.state = TaskState.Pending
    self.task_set[task.id] = task
    self.pending_list.append(task.id)

    if self.task_pickler is not None:
      self.task_pickler.update_task_node(
        task.id,
        serialize_task(task))

    logger.info('add task: %s' % str(task))
    return task.id

  def remove(self, task_id):
    if task_id not in self.task_set:
      logger.error("can not remove non-exist task id: %s" % task_id)
      return

    state = self.task_set[task_id].state
    if state == TaskState.Pending:
      self.pending_list.remove(task_id)
    elif state == TaskState.Scheduled:
      self.scheduled_list.remove(task_id)
    elif state == TaskState.Running:
      self.running_list.remove(task_id)
    elif state == TaskState.Error:
      self.error_list.remove(task_id)
    elif state == TaskState.Finish:
      self.finish_list.remove(task_id)
    else:
      logger.error("unknown task state, task id %s %s" % (task_id, state))
      return

    if self.task_pickler is not None:
      self.task_pickler.remove_task_node(task_id)

    task_info = str(self.task_set[task_id])
    del self.task_set[task_id]

    logger.info('remove task: %s' % task_info)

  def state_transfer(self, task_id, input_action = None):
    if task_id not in self.task_set:
      logger.error("non-exist task id: %s" % task_id)
      return

    if self.task_set[task_id].state == TaskState.Pending:
      self.pending_list.remove(task_id)
      self.scheduled_list.add(task_id)
      self.task_set[task_id].state = TaskState.Scheduled
    elif self.task_set[task_id].state == TaskState.Scheduled:
      self.scheduled_list.remove(task_id)
      self.running_list.add(task_id)
      self.task_set[task_id].state = TaskState.Running
    elif self.task_set[task_id].state == TaskState.Running:
      self.running_list.remove(task_id)
      if input_action == TaskTransferInput.Error:
        self.error_list.add(task_id)
        self.task_set[task_id].state = TaskState.Error
      else:
        self.finish_list.add(task_id)
        self.task_set[task_id].state = TaskState.Finish
    elif self.task_set[task_id].state == TaskState.Error:
      self.error_list.remove(task_id)
      if input_action == TaskTransferInput.Reschedule or \
         input_action == TaskTransferInput.Recover:
        self.pending_list.append(task_id)
        self.task_set[task_id].clear_offer()
        self.task_set[task_id].state = TaskState.Pending
      else:
        self.finish_list.add(task_id)
        self.task_set[task_id].state = TaskState.Finish
    else:
      logger.error("task_id: %s, unknown state: %s"
                   % (task_id, self.task_set[task_id].state))
      return

    if self.task_pickler is not None:
      self.task_pickler.update_task_node(
        task_id,
        serialize_task(self.task_set[task_id]))

if __name__ == '__main__':
  import logging
  import unittest
  from matrix.core.task_pickler import TaskPickler
  from kazoo.client import KazooClient

  class TaskCollectionTest(unittest.TestCase):
    def setUp(self):
      self.zk_client = KazooClient(hosts = "223.202.46.153:2181")
      self.zk_client.start()
      self.pickler = TaskPickler("MatrixTest", "1", self.zk_client)
      logger.setLevel(logging.DEBUG)

    def tearDown(self):
      self.pickler.clear_metadata()
      self.zk_client.stop()

    def testStateTransfer(self):
      task = Task()
      task.constraint.cpus = 1
      task.constraint.mem = 1024
      task_collection = TaskCollection()
      self.assertEqual(task_collection.add(task), 0)
      self.assertEqual(task.state, TaskState.Pending)
      task_collection.state_transfer(0)
      self.assertEqual(task.state, TaskState.Scheduled)
      task_collection.state_transfer(0)
      self.assertEqual(task.state, TaskState.Running)
      task_collection.state_transfer(0)
      self.assertEqual(task.state, TaskState.Finish)
      task_collection.remove(0)
      task_collection.remove(1)

    def testZKTrunk(self):
      task1 = Task()
      task1.constraint.cpus = 1
      task1.constraint.mem = 1024
      task2 = Task()
      task2.constraint.cpus = 2
      task2.constraint.mem = 2048
      task_collection = TaskCollection(self.pickler)  
      task_collection.add(task1)
      task_collection.add(task2)
      # test init from zk
      task_collection2 = TaskCollection(self.pickler)
      print task_collection2.pending_list
      task_collection2.init_from_zookeeper()
      print task_collection2.pending_list

  unittest.main()
