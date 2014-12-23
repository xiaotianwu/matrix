__author__ = 'xiaotian.wu'

from util import Enum

TaskPriority = Enum(['Low', 'Median', 'High', 'RealTime'])

TaskProperty = Enum(['Transfered', 'Exclusive', 'AutoRestart'])

TaskState = Enum(['Pending', 'Scheduled', 'Running', 'Error', 'Finish'])

class TaskConstraint:
  cpus = None
  mem = None
  rack = None
  host = None

class Task:
  id = None
  name = None
  constraint = None
  priority = None
  property = None
  state = None
  strategy = None
  command = None
  offer = None
  slave_id = None
  executor_id = None

if __name__ == '__main__':
  task = Task()
  task.priority = TaskPriority.Low
  print task.priority
