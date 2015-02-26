__author__ = 'xiaotian.wu@chinacache.com'

import json

from matrix.core.util import Enum, object_to_dict, dict_to_object

# Task Priority is not used now
TaskPriority = Enum(['Low', 'Median', 'High', 'RealTime'])
TaskProperty = Enum(['Stateless', 'Exclusive', 'AutoRecover'])
TaskState = Enum(['Pending', 'Scheduled', 'Running', 'Error', 'Finish'])
TaskTransferInput = Enum(["Error", "Recover", "Reschedule"])

class TaskConstraint:
  '''Basic constraint for task. Sometime we need to assign task to
     the specified host for some reason. For example, a Kafka node
     running on 192.168.0.1 keeps its data on local storage device
     and there is no other node has enough space to replicate it, we
     have to limit it to the node 192.168.0.1. If the field of task
     constraint has been filled, the task will only be run if and only
     if the specified host has enough resources(cpu and memory).
     And, rack constraint is not used now.'''
  def __init__(self,
               cpus = -1,
               mem = -1,
               rack = -1,
               host = ""):
    self.cpus = cpus
    self.mem = mem
    self.rack = rack
    self.host = host

  def __str__(self):
    return "cpus: %s, memory: %s, rack: %s, host: %s" \
           % (self.cpus, self.mem, self.rack, self.host)

class Task:
  '''The necessary information for task. Explantion for some comfusable
     field. ro_volumes refers to read-only volumes mapping for Docker
     containers, and rw_volumes refers to read-write. IP is used to
     service discovery. If a task run as a web-service, we can use the
     name/ip pair to locate it'''
  def __init__(self,
               id = -1,
               name = "",
               docker_image = "",
               constraint = None,
               priority = TaskPriority.Low,
               property = [],
               state = TaskState.Pending,
               command = "",
               ro_volumes = {},
               rw_volumes = {},
               offer_id = -1,
               slave_id = -1,
               executor_id = -1,
               ip = ""):
    self.id = id
    self.name = name
    self.docker_image = docker_image
    self.constraint = TaskConstraint() if constraint is None else constraint
    self.priority = priority
    self.property = property
    self.state = state
    self.command = command
    self.ro_volumes = ro_volumes
    self.rw_volumes = rw_volumes
    self.offer_id = offer_id
    self.slave_id = slave_id
    self.executor_id = executor_id
    self.ip = ip

  def __str__(self):
    return "id: %s, name: %s, docker image: %s, constraint: {%s}, " \
           % (self.id, self.name, self.docker_image, str(self.constraint)) + \
           " priority: %s, property: %s, state: %s, command: %s, " \
           % (self.priority, self.property, self.state, self.command) + \
           " read-only volumes: %s, read-write volumes: %s, " \
           % (self.ro_volumes, self.rw_volumes) + \
           " offer id: %s, slave id: %s, executor id: %s, ip: %s" \
           % (self.offer_id, self.slave_id, self.executor_id, self.ip)

  def clear_offer(self):
    self.offer_id = -1
    self.slave_id = -1
    self.executor_id = -1

def serialize_task(task):
  return json.dumps(task, default = object_to_dict)

def deserialize_task(json_str):
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
      t1.ro_volumes = {'/a':'/b'}
      t2 = serialize_task(t1)
      print t2
      t3 = deserialize_task(t2)
      self.assertEqual(t1.constraint.cpus, t3.constraint.cpus)
      self.assertEqual(t1.constraint.mem, t3.constraint.mem)
      print str(t3)

  unittest.main()
