__author__ = 'xiaotian.wu@chinacache.com'

from matrix.core.util import Enum

TaskPriority = Enum(['Low', 'Median', 'High', 'RealTime'])
TaskProperty = Enum(['Stateless', 'Exclusive', 'AutoRecover'])
TaskState = Enum(['Pending', 'Scheduled', 'Running', 'Error', 'Finish'])
TaskTransferInput = Enum(["Error", "Recover", "Reschedule"])

class TaskConstraint:
  def __init__(self):
    self.cpus = None
    self.mem = None
    self.rack = None
    self.host = None

class Task:
  def __init__(self):
    self.id = None
    self.name = None
    self.docker_image = None
    self.constraint = TaskConstraint()
    self.priority = None
    self.property = []
    self.state = None
    self.strategy = None
    self.command = None
    self.offer_id = None
    self.slave_id = None
    self.executor_id = None

  def clear_offer(self):
    self.offer_id = None
    self.slave_id = None
    self.executor_id = None

if __name__ == '__main__':
  import unittest

  class TaskTest(unittest.TestCase):
    def testSet(self):
      task = Task()
      task.priority = TaskPriority.Low
      print task.priority
      print TaskTransferInput.error

  unittest.main()
