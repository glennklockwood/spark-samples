#!/usr/bin/env python
#
#  Report basic performance metrics from the Spark event log.  Enable event
#  logging by passing the following to the spark-submit command:
#
#    --conf spark.eventLog.enabled=true \
#    --conf spark.eventLog.dir=$SCRATCH/spark/spark_event_logs 
#

import sys
import json

_BYTES_TO_MIB = 1.0 / 1024.0 / 1024.0

fp = open( sys.argv[1], 'r' )
for line in fp:
    blob = json.loads( line )
    if blob['Event'] == 'SparkListenerTaskEnd':
        elapsed_ms = int(blob['Task Info']['Finish Time']) -  int(blob['Task Info']['Launch Time'])
        total_mb_in = int(blob['Task Metrics']['Input Metrics']['Bytes Read']) * _BYTES_TO_MIB 
        print "Task %6d read at %.2f MiB/sec with %.2f+%.2f MiB spilled" % ( 
            blob['Task Info']['Task ID'], 
            total_mb_in / elapsed_ms * 1000.0,
            int(blob['Task Metrics']['Memory Bytes Spilled']) * _BYTES_TO_MIB,
            int(blob['Task Metrics']['Disk Bytes Spilled']) * _BYTES_TO_MIB
            )
