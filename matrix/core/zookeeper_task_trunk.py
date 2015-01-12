__author__ = 'xiaotian.wu'

import logging

from kazoo.client import KazooClient

logging.basicConfig()

class ZookeeperTaskTrunk:
  def __init__(self, framework_name, framework_id, zk_hosts):
    self.zk = KazooClient(hosts = zk_hosts)
    self.zk.start()

    self.framework_name = framework_name
    self.framework_id = framework_id
    self.zk_root = '/%s/%s/' % (framework_name, framework_id)
    self.task_set_root = self.zk_root + 'task_set/' 

    if not self.zk.exists(self.zk_root):
      self.zk.ensure_path(self.zk_root)
    if not self.zk.exists(self.task_set_root):
      self.zk.ensure_path(self.task_set_root)

  def remove_task_node(self, task_id):
    task_node = self.task_set_root + str(task_id)
    if self.zk.exists(task_node):
      self.zk.delete(task_node)

  def update_task_node(self, task_id, data):
    task_node = self.task_set_root + str(task_id)
    if not self.zk.exists(task_node):
      self.zk.create(task_node)
    self.zk.set(task_node, data)

  def get_task_data(self, task_id):
    task_node = self.task_set_root + str(task_id)
    if not self.zk.exists(task_node):
      return None
    else:
      data = self.zk.get(task_node)
      if data is not None and len(data) > 0:
        return data[0]
      else:
        return None

  def __del__(self):
    self.zk.stop()

  def clear_metadata(self):
    if self.zk.exists(self.zk_root):
      self.zk.delete(self.zk_root, recursive = True)

if __name__ == '__main__':
  import unittest

  class ZookeeperTaskTrunkTest(unittest.TestCase):
    def testUpdate(self):
      zk_handler = ZookeeperTaskTrunk("MatrixTest", "1", "223.202.46.153:2181")
      zk_handler.update_task_node(1, "hello")
      self.assertEqual("hello", zk_handler.get_task_data(1))
      zk_handler.clear_metadata()

  unittest.main()
