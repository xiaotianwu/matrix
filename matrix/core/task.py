__author__ = 'xiaotian.wu@chinacache.com'

from matrix.core.util import Enum

TaskPriority = Enum(['Low', 'Median', 'High', 'RealTime'])
TaskProperty = Enum(['Stateless', 'Exclusive', 'AutoRestart'])
TaskState = Enum(['Pending', 'Scheduled', 'Running', 'Error', 'Finish'])

class TaskConstraint:
  cpus = None
  mem = None
  rack = None
  host = None

class Task:
  id = None
  name = None
  docker_image = None
  constraint = TaskConstraint()
  priority = None
  property = None
  state = None
  strategy = None
  command = None
  offer_id = None
  slave_id = None
  executor_id = None

if __name__ == '__main__':
  task = Task()
  task.priority = TaskPriority.Low
  print task.priority
