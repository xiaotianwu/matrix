__author__ = 'xiaotian.wu@chinacache.com'

from matrix.core.logger import logger

class TaskPickler:
  def __init__(self, framework_name, framework_id, zk_client):
    if zk_client is None:
      raise Exception("zk client not exists")
    self.zk = zk_client
    self.framework_name = framework_name
    self.framework_id = framework_id
    self.zk_root = '/%s/%s/' % (framework_name, framework_id)
    self.task_set_root = self.zk_root + 'task_set/' 
    logger.info("init task pickler, framework name: %s, framework id: %s, zk root: %s, task set root: %s"
                % (self.framework_name, self.framework_id, self.zk_root, self.task_set_root))

    if not self.zk.exists(self.zk_root):
      self.zk.ensure_path(self.zk_root)
    if not self.zk.exists(self.task_set_root):
      self.zk.ensure_path(self.task_set_root)

  def remove_task_node(self, task_id):
    task_node = self.task_set_root + str(task_id)
    if self.zk.exists(task_node):
      self.zk.delete(task_node)
    logger.debug("remove task node: %s success" % task_node)

  def update_task_node(self, task_id, data):
    task_node = self.task_set_root + str(task_id)
    if not self.zk.exists(task_node):
      self.zk.create(task_node)
    self.zk.set(task_node, data)
    logger.debug("update task node: %s success" % task_node)

  def get_all_task(self):
    tasks = []
    task_nodes = self.zk.get_children(self.task_set_root)
    for node in task_nodes:
      node_path = self.task_set_root + str(node)
      data = self.zk.get(node_path)
      if data is not None and len(data) > 0:
        tasks.append(data[0])
    return tasks

  def get_task(self, task_id):
    task_node = self.task_set_root + str(task_id)
    if not self.zk.exists(task_node):
      logger.error("can't get data from non-exist task node %s" % task_node)
      return None
    else:
      data = self.zk.get(task_node)
      if data is not None and len(data) > 0:
        return data[0]
      else:
        logger.error("task node: %s is empty" % task_node)
        return None

  def clear_metadata(self):
    if self.zk.exists(self.zk_root):
      self.zk.delete(self.zk_root, recursive = True)
    logger.warning("clear task pickler, framework name: %s, framework id: %s, zk root: %s, task set root: %s"
                   % (self.framework_name, self.framework_id, self.zk_root, self.task_set_root))

if __name__ == '__main__':
  import unittest
  from kazoo.client import KazooClient

  class TaskPicklerTest(unittest.TestCase):
    def setUp(self):
      self.zk_client = KazooClient(hosts = "223.202.46.153:2181")
      self.zk_client.start()

    def tearDown(self):
      self.zk_client.stop()

    def testBasicUsage(self):
      pickler = TaskPickler("MatrixTest", "1", self.zk_client)
      pickler.update_task_node(1, "hello")
      pickler.update_task_node(2, "thankyou")
      self.assertEqual("hello", pickler.get_task(1))
      self.assertEqual("thankyou", pickler.get_task(2))
      datas = pickler.get_all_task()
      self.assertTrue("hello" in datas)
      self.assertTrue("thankyou" in datas)
      pickler.clear_metadata()

  unittest.main()
