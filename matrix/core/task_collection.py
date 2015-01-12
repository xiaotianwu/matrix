__author__ = 'xiaotian.wu'

from collections import deque

from matrix.core.task import TaskState, TaskConstraint, TaskTransferInput, Task, task_to_json, json_to_task

class TaskCollection:
  def __init__(self, zk_task_trunk = None):
    self.pending_list = deque()
    self.scheduled_list = set()
    self.running_list = set()
    self.error_list = set()
    self.finish_list = set()
    self.task_set = {}
    self.zk_task_trunk = zk_task_trunk

  def add(self, task):
    if task.id == -1:
      raise Exception("task id not filled")
    task.state = TaskState.Pending
    self.task_set[task.id] = task
    self.pending_list.append(task.id)

    if self.zk_task_trunk is not None:
      self.zk_task_trunk.update_task_node(
        task.id,
        task_to_json(task))

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
      if self.zk_task_trunk is not None:
        self.zk_task_trunk.remove_task_node(task_id)

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

    if self.zk_task_trunk is not None:
      self.zk_task_trunk.update_task_node(
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
      task.constraint = TaskConstraint()
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

  unittest.main()
