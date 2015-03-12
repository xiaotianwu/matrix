#!/usr/bin/python

__author__ = 'yiji.liu@chinacache.com'

import os
import socket
import subprocess
import threading

from flask import Flask
from flask import request
from xml.etree import ElementTree

from matrix.core.logger import logger
from matrix.frontdoor.transcoder.process_template import process_template

ip = socket.gethostbyname(socket.gethostname())
app = Flask(__name__)
task_map = {}
task_map_lock = threading.Lock()

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
  print response
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
  #command = '/ffmpeg/ffmpeg -i %s -acodec copy -vcodec copy -f flv %s' % (input, out)
  command = process_template(input, out, bitrate, resolution, vcodec, acodec)
  curl_command = "curl http://%s:30000/matrix/add -d \"name=%s\" -d \"image=%s\" -d \"command=%s\" -d \"cpus=%s\" -d \"mem=%s\" -X POST" \
                 % (ip, input + ' ' + out, "180.97.185.35:5000/transcoder_v4", command, 1, 1024)
  logger.info(curl_command)
  return_msg = subprocess.Popen(curl_command, stdout = subprocess.PIPE, shell = True)
  task_id = return_msg.stdout.read()
  task_id = int(task_id.strip("\""))
  logger.info(task_id)
  task_map_lock.acquire()
  if input not in task_map:
    task_map[input] = []
  task_map[input].append(task_id)
  task_map_lock.release()
  return True

def stop_task(input):
  task_map_lock.acquire()
  if input in task_map:
    for task_id in task_map[input]:
      curl_command = "curl http://%s:30000/matrix/remove/%s -X POST" % (ip, task_id)
      logger.info(curl_command)
      os.system(curl_command)
  task_map[input] = []
  task_map_lock.release()
  return True

app.run("0.0.0.0", port = 40000)
