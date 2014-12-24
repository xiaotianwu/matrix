__author__ = 'xiaotian.wu'

from collections import deque
from task import *
from task_selector import TaskSelector

class TaskManager:
  def __init__(self):
    self._pending_list = deque()
    self._scheduled_list = set()
    self._running_list = set()
    self._error_list = set()
    self._finish_list = set()
    self._task_set = {}

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
    print 'offers: ', offers
    print 'pending tasks: ', pending_tasks
    selector = TaskSelector(offers, pending_tasks)
    scheduled_tasks = selector.match()
    print 'scheduled tasks: ', scheduled_tasks
    return scheduled_tasks

  def get_task(self, task_id):
    if task_id in self._task_set:
      return self._task_set[task_id]
    else:
      raise Exception("task id: %s does not exist" % task_id)

if __name__ == '__main__':
  task_manager = TaskManager()
  task = Task()
  task.id = 1
  task.constraint = TaskConstraint()
  task.constraint.cpus = 1
  task.constraint.mem = 1024
  task_manager.add(task)
  print task.state
  task_manager.move_to_next_state(1)
  print task.state
  task_manager.move_to_next_state(1)
  print task.state
  task_manager.move_to_next_state(1)
  print task.state
  task_manager.remove(1)
