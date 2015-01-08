__author__ = 'xiaotian.wu@chinacache.com'

from matrix.core.util import Enum

TaskPriority = Enum(['Low', 'Median', 'High', 'RealTime'])
TaskProperty = Enum(['Stateless', 'Exclusive', 'AutoRestart'])
TaskState = Enum(['Pending', 'Scheduled', 'Running', 'Error', 'Finish'])

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
    self.property = None
    self.state = None
    self.strategy = None
    self.command = None
    self.offer_id = None
    self.slave_id = None
    self.executor_id = None

if __name__ == '__main__':
  task = Task()
  task.priority = TaskPriority.Low
  print task.priority
