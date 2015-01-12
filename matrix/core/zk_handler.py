__author__ = 'xiaotian.wu'

import logging

from kazoo.client import KazooClient

logging.basicConfig()

class ZookeeperHandler:
  def __init__(self, framework_name, framework_id, zk_hosts):
    self.zk = KazooClient(hosts = zk_hosts)
    self.zk.start()

    self.framework_name = framework_name
    self.framework_id = framework_id
    self.zk_root = '/%s/%s/' % (framework_name, framework_id)
    self.task_set_root = self.zk_root + 'task_set/' 
    self.pending_list_root = self.zk_root + 'pending_list/' 
    self.scheduled_list_root = self.zk_root + 'scheduled_list/' 
    self.running_list_root = self.zk_root + 'running_list/' 
    self.error_list_root = self.zk_root + 'error_list/' 
    self.finish_list_root = self.zk_root + 'finish_list/'

    if not self.zk.exists(self.zk_root):
      self.zk.create(self.zk_root)
    if not self.zk.exists(self.task_set_root):
      self.zk.create(self.task_set_root)
    if not self.zk.exists(self.pending_list_root):
      self.zk.create(self.pending_list_root)
    if not self.zk.exists(self.scheduled_list_root):
      self.zk.create(self.scheduled_list_root)
    if not self.zk.exists(self.running_list_root):
      self.zk.create(self.running_list_root)
    if not self.zk.exists(self.error_list_root):
      self.zk.create(self.error_list_root)
    if not self.zk.exists(self.finish_list_root):
      self.zk.create(self.finish_list_root)

  def __del__(self):
    self.zk.stop()
