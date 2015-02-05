__author__ = 'xiaotian.wu@chinacache.com'

from matrix.core.task import Task, TaskProperty

def health():
  return "I am healthy"

def add(framework, task_name,
        docker_image, command,
        cpus, mem, host):
  task = Task()
  if len(task_name) == 0:
    return False
  if cpus <= 0 or mem <= 0:
    return False
  task.docker_image = docker_image
  task.command = command
  task.name = task_name
  task.constraint.cpus = cpus
  task.constraint.mem = mem
  task.constraint.host = host
  framework.add(task)
  return True

def delete(framework, task_id):
  framework.delete(task_id)
  return True

def get(framework, task_id):
  return framework.get(task_id)
