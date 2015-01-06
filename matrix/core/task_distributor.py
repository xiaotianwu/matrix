__author__ = 'xiaotian.wu@chinacache.com'

import rbtree

from matrix.core.logger import logger
from matrix.core.offer import get_cpus_from_offer, get_mem_from_offer

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
      cpus = get_cpus_in_offer(offer)
      mem = get_mem_in_offer(offer)
      weight = self.calculate_weight(cpus, mem)
      self.offers_mapping[weight] = (offer.id, cpus, mem, offer.hostname)
      self.offers_reverse_mapping[offer.hostname] = (offer, weight)

  def calculate_weight(self, cpus, mem): 
    weight = 0 - cpus * CPU_FACTOR + mem * MEM_FACTOR / 1024
    # avoid same value appeared in rb tree
    # such that, the ealier offer will have smaller weight
    while self.offers_tree.has_key(weight):
      weight += 0.0001
    return weight

  def assign(self):
    '''priority: specified host > cpu/mem'''
    scheduled_task = []

    for task in self.tasks:
      (offer.id, cpus, mem, offer.hostname) = self.offers_mapping.pop()
      choose = False
      if not task.constraint.host.strip():
        if cpus > task.constraint.cpus and mem > task.constraint.mem:
          choose = True
      else:
        if task.constraint.host == offer.hostname:
          if cpus > task.constraint.cpus and mem > task.constraint.mem:
            choose = True

      if choose is True:
        offer = self.offers_reverse_mapping[offer.hostname]
        task.offer = offer
        scheduled_task.append(task)
        cpus -= task.constraint.cpus
        mem -= task.constraint.mem
        weight = self.calculate_weight(cpus, mem)
        self.offers_mapping[weight] = (offer.id, cpus, mem, offer.hostname)
        self.offers_reverse_mapping[offer.hostname] = (offer, weight)
        break

    return scheduled_task

if __name__ == '__main__':
  import unittest

  class TaskDistributorTest(unittest.TestCase):
    def setUp(self):
      self.offer = mesos_pb2.Offer()
      cpus_res = self.offer.resources.add()
      cpus_res.name = 'cpus'
      cpus_res.type = mesos_pb2.Value.SCALAR
      cpus_res.scalar.value = 3

    def testDistribution1(self):
      pass

    def testDistribution2(self):
      pass

  unittest.main()
