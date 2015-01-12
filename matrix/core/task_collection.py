__author__ = 'xiaotian.wu'

from collections import deque

from matrix.core.task import (TaskState, TaskConstraint,
                              TaskTransferInput, Task,
                              serialize_task, deserialize_task)
 
from matrix.core.zookeeper_task_trunk import ZookeeperTaskTrunk

class TaskCollection:
  def __init__(self, zookeeper_task_trunk = None):
    self.pending_list = deque()
    self.scheduled_list = set()
    self.running_list = set()
    self.error_list = set()
    self.finish_list = set()
    self.task_set = {}
    self.zookeeper_task_trunk = zookeeper_task_trunk

  def init_from_zookeeper(self):
    if self.zookeeper_task_trunk is None:
      raise Exception("zookeeper instance not exists")
    all_task_data = self.zookeeper_task_trunk.get_all_task_data()
    all_task = [deserialize_task(str(data)) for data in all_task_data]
    for task in all_task:
      self.task_set[task.id] = task
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

  def add(self, task):
    if task.id == -1:
      raise Exception("task id not filled")
    task.state = TaskState.Pending
    self.task_set[task.id] = task
    self.pending_list.append(task.id)

    if self.zookeeper_task_trunk is not None:
      self.zookeeper_task_trunk.update_task_node(
        task.id,
        serialize_task(task))

  def remove(self, task_id):
    if task_id in self.task_set:
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
        raise Exception("unknown task state, task id %s %s" % (task_id, state))
      del self.task_set[task_id]
      if self.zookeeper_task_trunk is not None:
        self.zookeeper_task_trunk.remove_task_node(task_id)

  def move_to_next_state(self, task_id, input_action = None):
    if task_id not in self.task_set:
      raise Exception("task id: %s not in task set" % task_id)
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
      raise Exception("task_id: %s unknown state: %s"
                       %(task_id, self.task_set[task_id].state))

    if self.zookeeper_task_trunk is not None:
      self.zookeeper_task_trunk.update_task_node(
        task_id,
        self.serialize(self.task_set[task_id]))

if __name__ == '__main__':
  import unittest
  import mesos.interface
  from mesos.interface import mesos_pb2

  class TaskCollectionTest(unittest.TestCase):
    def testStateTransfer(self):
      task = Task()
      task.id = 1
      task.constraint.cpus = 1
      task.constraint.mem = 1024
      task_collection = TaskCollection()
      task_collection.add(task)
      self.assertEqual(task.state, TaskState.Pending)
      task_collection.move_to_next_state(1)
      self.assertEqual(task.state, TaskState.Scheduled)
      task_collection.move_to_next_state(1)
      self.assertEqual(task.state, TaskState.Running)
      task_collection.move_to_next_state(1)
      self.assertEqual(task.state, TaskState.Finish)
      task_collection.remove(1)

    def testZKTrunk(self):
      t1 = Task()
      t1.constraint.cpus = 2
      t1.constraint.mem = 3
      t2 = serialize_task(t1)
      print t2
      t3 = deserialize_task(t2)
      self.assertEqual(t1.constraint.cpus, t3.constraint.cpus)
      self.assertEqual(t1.constraint.mem, t3.constraint.mem)

#      task1 = Task()
#      task1.id = 1
#      task1.constraint.cpus = 1
#      task1.constraint.mem = 1024
#      task2 = Task()
#      task2.id = 2
#      task2.constraint.cpus = 2
#      task2.constraint.mem = 2048
#      zk_trunk = ZookeeperTaskTrunk("MatrixTest", "1", "223.202.46.153:2181")
#      task_collection = TaskCollection(zk_trunk)  
#      task_collection.add(task1)
#      task_collection.add(task2)
#      # just for unittest, do not call it directly
#      data1 = zk_trunk.get_task_data(1)
#      print 'data1 =', data1
#      print deserialize_task(data1)
#      # print zk_trunk.get_task_data(2)
#      # test init from zk
#      # task_collection2 = TaskCollection(zk_trunk)
#      # print task_collection2.pending_list
#      # task_collection2.init_from_zookeeper()
#      # print task_collection2.pending_list
#      zk_trunk.clear_metadata()

  unittest.main()
