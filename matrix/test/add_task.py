#!/usr/bin/python

from xmlrpclib import ServerProxy

matrix = ServerProxy("http://223.202.46.132:30000")

#matrix.add(1, "WalleMonitor", "cpdc/walle_monitor", "/walle_monitor_0_2_1/monitor/main.py --kafka_hosts=223.203.199.153:9092,223.203.199.152:9092,223.203.199.150:9092,223.203.199.149:9092,223.203.199.148:9092 --topic=final-log --ip_mapping_file=/walle_monitor_0_2_1/monitor/ipmapping.conf", 1, 2048, "MIS-BJ-6-5A2")

matrix.add(2, "StormNimbus", "cpdc/storm_nimbus", "su storm&&nohup /usr/lib/apache-storm-0.9.3/bin/storm nimbus", 2, 2048, "MIS-BJ-6-5A1")
