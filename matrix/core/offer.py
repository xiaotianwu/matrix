__author__ = 'xiaotian.wu'

import mesos.interface
from mesos.interface import mesos_pb2

def _get_target_from_offer(offer, target):
  for res in offer.resources:
    if res.name == target and res.type == mesos_pb2.Value.SCALAR:
      return res.scalar.value
  raise Exception("target: %s not exists" % target)

def get_cpus_from_offer(offer):
  return float(str(_get_target_from_offer(offer, "cpus")))

def get_mem_from_offer(offer):
  return float(str(_get_target_from_offer(offer, "mem")))

def get_disk_from_offer(offer):
  return float(str(_get_target_from_offer(offer, "disk")))

def get_ports_from_offer(offer):
  return float(str(_get_target_from_offer(offer, "ports")))
