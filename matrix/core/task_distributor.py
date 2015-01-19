__author__ = 'xiaotian.wu@chinacache.com'

import rbtree

from matrix.core.logger import logger
from matrix.core.offer import get_cpus_from_offer, get_mem_from_offer
from matrix.core.task import Task

CPU_FACTOR = 0.6
MEM_FACTOR = 0.4

class TaskDistributor:
  def __init__(self, offers, tasks):
    self.offers = offers
    self.tasks = tasks
    self.offers_mapping = rbtree.rbtree()
    self.offers_reverse_mapping = {}
    for offer in self.offers:
      logger.debug("offer detail: %s" % offer)
      cpus = get_cpus_from_offer(offer)
      mem = get_mem_from_offer(offer)
      weight = self.calculate_weight(cpus, mem)
      self.offers_mapping[weight] = (offer.id,
                                     offer.slave_id.value,
                                     cpus,
                                     mem,
                                     offer.hostname)
      self.offers_reverse_mapping[offer.hostname] = weight

  def debug_info(self):
    logger.debug('--------------------offers mapping-------------------')
    for i in self.offers_mapping.iternodes():
      logger.debug(str(i.key) + '\t' + str(i.value))
    logger.debug('------------offers reverse mapping-------------------')
    for key in self.offers_reverse_mapping.keys():
      logger.debug(str(key) + '\t' + str(self.offers_reverse_mapping[key]))

  def calculate_weight(self, cpus, mem): 
    weight = -(cpus * CPU_FACTOR + mem / 1024 * MEM_FACTOR)
    # avoid same value appeared in rb tree, the ealier offer will have smaller weight
    while self.offers_mapping.has_key(weight):
      weight += 0.0001
    return weight

  def assign(self):
    '''priority: specified host > cpu/mem'''
    scheduled_task = []

    for task in self.tasks:
      choose = False
      offer_id, slave_id, cpus, mem, hostname = None, None, None, None, None
      if task.constraint.host is not None and len(task.constraint.host) > 0:# and not task.constraint.host.strip():
        if task.constraint.host not in self.offers_reverse_mapping:
          continue
        iterator = iter(self.offers_mapping)
        iterator.goto(self.offers_reverse_mapping[task.constraint.host])
        offer_id, slave_id, cpus, mem, hostname = iterator.get()
        try:
          iterator.delete()
        except StopIteration:
          pass
        if cpus >= task.constraint.cpus and mem >= task.constraint.mem:
          choose = True
      else:
        offer_id, slave_id, cpus, mem, hostname = self.offers_mapping.pop()
        if cpus >= task.constraint.cpus and mem >= task.constraint.mem:
          choose = True

      if choose is True:
        task.offer_id = offer_id.value
        task.slave_id = slave_id
        scheduled_task.append(task)
        cpus -= task.constraint.cpus
        mem -= task.constraint.mem
        logger.info("assign task id: %s to offer id: %s, slave id: %s"
                    %(task.id, task.offer_id, task.slave_id))
        weight = self.calculate_weight(cpus, mem)
        self.offers_mapping[weight] = (offer_id, slave_id, cpus, mem, hostname)
        self.offers_reverse_mapping[hostname] = weight
        self.debug_info()

    return scheduled_task

if __name__ == '__main__':
  import copy
  import unittest
  import mesos.interface
  from mesos.interface import mesos_pb2

  class TaskDistributorTest(unittest.TestCase):
    def setUp(self):
      offer1 = mesos_pb2.Offer()
      offer1.id.value = '1'
      offer1.slave_id.value = '1'
      offer1.hostname = 'A'
      cpus_res = offer1.resources.add()
      cpus_res.name = 'cpus'
      cpus_res.type = mesos_pb2.Value.SCALAR
      cpus_res.scalar.value = 3
      mem_res = offer1.resources.add()
      mem_res.name = 'mem'
      mem_res.type = mesos_pb2.Value.SCALAR
      mem_res.scalar.value = 3072
      offer2 = mesos_pb2.Offer()
      offer2.id.value = '2'
      offer2.slave_id.value = '2'
      offer2.hostname = 'B'
      cpus_res = offer2.resources.add()
      cpus_res.name = 'cpus'
      cpus_res.type = mesos_pb2.Value.SCALAR
      cpus_res.scalar.value = 4
      mem_res = offer2.resources.add()
      mem_res.name = 'mem'
      mem_res.type = mesos_pb2.Value.SCALAR
      mem_res.scalar.value = 4096
      offer3 = mesos_pb2.Offer()
      offer3.id.value = '3'
      offer3.slave_id.value = '3'
      offer3.hostname = 'C'
      cpus_res = offer3.resources.add()
      cpus_res.name = 'cpus'
      cpus_res.type = mesos_pb2.Value.SCALAR
      cpus_res.scalar.value = 5
      mem_res = offer3.resources.add()
      mem_res.name = 'mem'
      mem_res.type = mesos_pb2.Value.SCALAR
      mem_res.scalar.value = 5120
      self.offers = [offer1, offer2, offer3]

    def testInialize(self):
      task_distributor = TaskDistributor(self.offers, None)
      task_distributor.debug_info()

    def testDistribution1(self):
      task1 = Task()
      task1.id = '1'
      task1.constraint.cpus = 2
      task1.constraint.mem = 3072
      task2 = Task()
      task2.id = '2'
      task2.constraint.cpus = 3
      task2.constraint.mem = 2048
      task3 = Task()
      task3.id = '3'
      task3.constraint.cpus = 1
      task3.constraint.mem = 1024
      tasks = [task1, task2, task3]
      offers = copy.deepcopy(self.offers)
      task_distributor = TaskDistributor(offers, tasks)
      task_distributor.assign()
      self.assertEqual(task1.offer_id.value, '3')
      self.assertEqual(task2.offer_id.value, '2')
      self.assertEqual(task3.offer_id.value, '1')

    def testDistribution2(self):
      task1 = Task()
      task1.id = '1'
      task1.constraint.cpus = 2
      task1.constraint.mem = 3072
      task1.constraint.host = 'A'
      task2 = Task()
      task2.id = '2'
      task2.constraint.cpus = 3
      task2.constraint.mem = 2048
      task3 = Task()
      task3.id = '3'
      task3.constraint.cpus = 1
      task3.constraint.mem = 1024
      tasks = [task1, task2, task3]
      offers = copy.deepcopy(self.offers)
      task_distributor = TaskDistributor(offers, tasks)
      task_distributor.assign()
      self.assertEqual(task1.offer_id.value, '1')
      self.assertEqual(task2.offer_id.value, '3')
      self.assertEqual(task3.offer_id.value, '2')

  unittest.main()
