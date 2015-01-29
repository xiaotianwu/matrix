#!/usr/bin/python

__author__ = 'xiaotian.wu@chinacache.com'

import socket
import threading

from flask import Flask
from flask import request
from xml.etree import ElementTree

from kazoo.client import KazooClient
from matrix.core.config import config
from matrix.core.framework import MatrixFramework
from matrix.core.logger import logger
from matrix.core.task import Task, TaskProperty

app = Flask(__name__)
ip = socket.gethostbyname(socket.gethostname())
port = 30000
zk_client = KazooClient(hosts = "223.202.46.153:2181")
zk_client.start()
framework_name = "Matrix"
framework_id = "1"
framework = MatrixFramework("223.202.46.132:5050", framework_name, framework_id, zk_client)
task_map = {}
task_map_lock = threading.Lock()

def add(task_name, docker_image, command, cpus, mem, host):
  task = Task()
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
  return framework.add(task)

def delete(task_id):
  framework.delete(task_id)
  return True

def get(task_id):
  return framework.get(task_id)

def list_all():
  return True

def start_xml_service():
  logger.info('start matrix web service at host %s:%s' %(ip, port))

def leader():
  logger.info("start leader mode")
  framework.start()
  app.run(host = ip, port = 6000)

@app.route('/')
def hello_world():
  return 'Hello World!'

@app.route('/api/lanxun/start', methods=['POST'])
def start_transcoder():
  data = request.data
  for d1,x in request.headers.items():
    logger.debug("key:" + d1 + ",value:" + str(x))
  if not data:
    logger.debug(''.join(request.environ['wsgi.input'].readlines()))
    request_body_size = int(request.environ.get('CONTENT_LENGTH', 0))
    data = request.environ["wsgi.input"].read(request_body_size)

  response = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
  if data and parse_cmd(data, "start"):
    response += "<success></success>"
  else:
    response += "<errors><error>unknown error.</error></errors>"
  return response

@app.route('/api/lanxun/stop', methods=['POST'])
def stop_transcoder():
  data = request.data
  for d1,x in request.headers.items():
    logger.debug("key:" + d1 + ",value:" + str(x))
  if not data:
    logger.debug(''.join(request.environ['wsgi.input'].readlines()))
    request_body_size = int(request.environ.get('CONTENT_LENGTH', 0))
    data = request.environ["wsgi.input"].read(request_body_size)

  response = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
  if data and parse_cmd(data, "stop"):
    response += "<success></success>"
  else:
    response += "<errors><error>unknown error.</error></errors>"
  return response
      
def parse_cmd(xmldata, type):
  try:
    logger.info(xmldata)
    root = ElementTree.fromstring(xmldata)
  except Exception, e:
    logger.error("Error: cannot parse xml data")
    return False
  if root is None:
    logger.error("parse failed")
    return False

  url = None
  for child in root:
    if child.tag == "url":
      url = child.text
    if url:
      break;
  if not url:
    logger.error("not exist url")
    return False

  if type == "stop":
    return stop_task(url)
  for output in root:
    if output.tag == "output":
      for item in output:
        outstr = None
        bitratestr = None
        resolstr = None
        vcodecstr = "h264"
        acodecstr = "aac"
        for child in item:
          if child.tag == "out":
            outstr = child.text
          elif child.tag == "bitrate":
            bitratestr = child.text
          elif child.tag == "resolution":
            resolstr = child.text
          elif child.tag == "vcodec":
            vcodecstr = child.text
          elif child.tag == "acodec":
            acodecstr = child.text
        if outstr and bitratestr:
          #create command
          start_task(url, outstr, bitratestr, resolstr, vcodecstr, acodecstr)
  return True

#input: rtmp://test:1935/live/stream
#out: rtmp://test:1935/output/stream_500k
#bitrate: 500(k)
#resolution: 704*576
#vcodec: h264
#acodec: aac
def start_task(input, out, bitrate, resolution, vcodec, acodec):
  command = '/ffmpeg/ffmpeg -i %s -acodec copy -vcodec copy -f flv %s' % (input, out)
  logger.info(command)
  task_id = add(input + ' ' + out, "223.202.46.132:5000/transcoder", command, 1, 1024, None)
  logger.info("task_id:%s" % task_id)
  task_map_lock.acquire()
  logger.info(input)
  if input not in task_map:
    task_map[input] = []
  task_map[input].append(task_id)
  task_map_lock.release()
  return True

def stop_task(input):
  task_map_lock.acquire()
  if input in task_map:
    for task_id in task_map[input]:
      delete(task_id)
  task_map_lock.release()
  return True

if __name__ == '__main__':
  logger.info("start election")
  election = zk_client.Election(framework_name + "/" + framework_id, ip)
  election.run(leader)
