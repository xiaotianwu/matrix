#!/usr/bin/python

__author__ = 'xiaotian.wu@chincache.com'

from optparse import OptionParser

from flask import Flask
from flask import render_template

from matrix.core.zookeeper_task_trunk import ZookeeperTaskTrunk

app = Flask(__name__)
zk_task_trunk = None

@app.route('/taskinfo/')
@app.route('/taskinfo/<task_id>')
def task_info(task_id = None):
  if task_id is None:
    task = zk_task_trunk.get_all_task_data()
  else:
    task = zk_task_trunk.get_task_data(task_id)
  return render_template('taskinfo.html', task = task)

def parse_options():
  parser = OptionParser()
  parser.add_option("-o", "--host", type = "string",
                    dest = "host", default = "223.202.46.132", help = "host")
  parser.add_option("-p", "--port", type = "int",
                    dest = "port", default = 20000, help = "port")
  parser.add_option("-f", "--framework_name", type = "string",
                    dest = "framework_name", default = "MatrixTest", help = "framework name")
  parser.add_option("-i", "--framework_id", type = "string",
                    dest = "framework_id", default = "1", help = "framework id")
  parser.add_option("-z", "--zk_hosts", type = "string",
                    dest = "zk_hosts", default = "223.202.46.153:2181", help = "zk hosts")
  (options, args) = parser.parse_args()
  return options

if __name__ == '__main__':
  options = parse_options()
  zk_task_trunk = ZookeeperTaskTrunk(options.framework_name, options.framework_id, options.zk_hosts)
  print options.host, options.port
  app.run(host = options.host, port = options.port)
