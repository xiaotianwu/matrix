__author__ = 'xiaotian.wu@chinacache.com'

import mesos.interface
from mesos.interface import mesos_pb2

def _get_resource_from_offer(offer, target):
  '''internal used'''
  for res in offer.resources:
    if res.name == target and res.type == mesos_pb2.Value.SCALAR:
      return res.scalar.value
  raise Exception("target: %s not exists in offer: %s" % (target, str(offer)))

def get_cpus_from_offer(offer):
  return float(str(_get_resource_from_offer(offer, "cpus")))

def get_mem_from_offer(offer):
  return float(str(_get_resource_from_offer(offer, "mem")))

def get_disk_from_offer(offer):
  return float(str(_get_resource_from_offer(offer, "disk")))

def get_ports_from_offer(offer):
  return float(str(_get_resource_from_offer(offer, "ports")))

if __name__ == '__main__':
  import unittest

  class GetResourcesTest(unittest.TestCase):
    def setUp(self):
      self.offer = mesos_pb2.Offer()
      cpus_res = self.offer.resources.add()
      cpus_res.name = 'cpus'
      cpus_res.type = mesos_pb2.Value.SCALAR
      cpus_res.scalar.value = 3

    def testGetCPU(self):
      self.assertEqual(get_cpus_from_offer(self.offer), 3.0)

  unittest.main()
