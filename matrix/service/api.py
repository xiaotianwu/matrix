__author__ = 'xiaotian.wu@chinacache.com'

from matrix.core.task import Task, TaskProperty

def health():
  return "I am healthy"

def add(framework,
        task_name,
        docker_image,
        command,
        ro_volumes,
        rw_volumes,
        cpus,
        mem,
        host):
  task = Task()
  if len(task_name) == 0:
    return False
  if cpus <= 0 or mem <= 0:
    return False

  if ro_volumes is not None:
    vols = ro_volumes.split(',')
    for mapping in vols:
      mapping = mapping.split(':')
      if len(mapping) == 2:
        task.ro_volumes[mapping[0]] = task.ro_volumes[mapping[1]]

  if rw_volumes is not None:
    vols = rw_volumes.split(',')
    for mapping in vols:
      mapping = mapping.split(':')
      if len(mapping) == 2:
        task.rw_volumes[mapping[0]] = task.rw_volumes[mapping[1]]

  print rw_volumes
  task.docker_image = docker_image
  task.command = command
  task.name = task_name
  task.constraint.cpus = cpus
  task.constraint.mem = mem
  task.constraint.host = host
  return framework.add(task)

def remove(framework, task_id):
  framework.remove(task_id)
  return True

def get(framework, task_id):
  return framework.get(task_id)
