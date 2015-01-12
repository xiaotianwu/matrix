__author__ = 'xiaotian.wu@chinacache.com'

import copy
import json

from matrix.core.util import Enum, object_to_dict, dict_to_object

TaskPriority = Enum(['Low', 'Median', 'High', 'RealTime'])
TaskProperty = Enum(['Stateless', 'Exclusive', 'AutoRecover'])
TaskState = Enum(['Pending', 'Scheduled', 'Running', 'Error', 'Finish'])
TaskTransferInput = Enum(["Error", "Recover", "Reschedule"])

class TaskConstraint:
  def __init__(self,
               cpus = -1,
               mem = -1,
               rack = -1,
               host = ""):
    self.cpus = cpus
    self.mem = mem
    self.rack = rack
    self.host = host

class Task:
  def __init__(self,
               id = -1,
               name = "",
               docker_image = "",
               constraint = None,
               priority = TaskPriority.Low,
               property = [],
               state = TaskState.Pending,
               command = "",
               offer_id = -1,
               slave_id = -1,
               executor_id = -1):
    self.id = id
    self.name = name
    self.docker_image = docker_image
    self.constraint = TaskConstraint() if constraint is None else constraint
    self.priority = priority
    self.property = property
    self.state = state
    self.command = command
    self.offer_id = offer_id
    self.slave_id = slave_id
    self.executor_id = executor_id

  def clear_offer(self):
    self.offer_id = -1
    self.slave_id = -1
    self.executor_id = -1

def task_to_json(task):
  return json.dumps(task, default = object_to_dict)

def json_to_task(json_str):
  return json.loads(json_str, object_hook = dict_to_object)

if __name__ == '__main__':
  import unittest
  import json

  class TaskTest(unittest.TestCase):
    def testSet(self):
      task = Task()
      task.priority = TaskPriority.Low
      print task.priority
      print TaskTransferInput.Error

    def testConvert(self):
      t1 = Task()
      t1.constraint.cpus = 2
      t1.constraint.mem = 3
      t2 = task_to_json(t1)
      print t2
      t3 = json_to_task(t2)
      self.assertEqual(t1.constraint.cpus, t3.constraint.cpus)
      self.assertEqual(t1.constraint.mem, t3.constraint.mem)

  unittest.main()
