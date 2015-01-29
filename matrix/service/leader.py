from kazoo.client import KazooClient

from matrix.core.config import config
from matrix.core.framework import MatrixFramework
from matrix.core.logger import logger
from matrix.core.task import Task, TaskProperty

ip = socket.gethostbyname(socket.gethostname())
port = 30000
zk_client = KazooClient(hosts = "223.202.46.153:2181")
zk_client.start()
framework_name = "Matrix"
framework_id = "1"
framework = MatrixFramework("223.202.46.132:5050", framework_name, framework_id, zk_client)
framework.start()

def add(task_name, docker_image, command, cpus, mem, host):
  task = Task()
  task.property.append(TaskProperty.AutoRecover)
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

def delete(task_id):
  framework.delete(task_id)
  return True

def get(task_id):
  return framework.get(task_id)

def list_all():
  return True

add('xxx', '223.202.46.132:5000/transcoder', '/ffmpeg/ffmpeg -i xxx', 1, 2048, None)
