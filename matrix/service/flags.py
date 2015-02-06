__author__ = 'xiaotian.wu@chinacache.com'

from optparse import OptionParser

def parse_flag():
  parser = OptionParser()
  parser.add_option("-a", "--zk",
                    type = "string", dest = "zk",
                    default = "180.97.185.37:2181,180.97.185.38:2181,180.97.185.39:2181", help = "zk hosts")
  parser.add_option("-b", "--mesos",
                    type = "string", dest = "mesos",
                    default = "10.20.72.130:5050", help = "mesos host")
  parser.add_option("-c", "--framework_name",
                    type = "string", dest = "framework_name",
                    default = "Matrix", help = "framework name")
  parser.add_option("-d", "--framework_id",
                    type = "string", dest = "framework_id",
                    default = "1", help = "framework id")
  parser.add_option("-e", "--rest_port",
                    type = "int", dest = "rest_port",
                    default = 30000, help = "for RESTFUL service")
  (flags, args) = parser.parse_args()
  return flags
