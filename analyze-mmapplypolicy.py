#!/usr/bin/env python
#
#  On NERSC systems, request an interactive session in cluster-compat mode:
#
#    salloc -N 4 -p debug -t 30:00 --ccm
#
#  then do
#
#    module load spark
#    start-all.sh
#    spark-submit --master $SPARKURL \
#                 --driver-memory=10G \
#                 --executor-memory=50G \
#                 ./analyze-mmapplypolicy.py
#    stop-all.sh
#

import os
from pyspark import SparkContext

#_INPUT_FILE      = '/scratch1/scratchdirs/glock/mmapplypolicy.sample'
_INPUT_FILE      = '/scratch1/scratchdirs/glock/mmapplypolicy.out'
_OUTPUT_DIR_BASE = '/scratch1/scratchdirs/glock/mmapplypolicy.sparked'
_NUM_PARTITIONS  = 100

### find a output directory that doesn't already exist
index_free = 0
output_dir = _OUTPUT_DIR_BASE + '.%d' % index_free
while os.path.isdir( output_dir ):
    index_free += 1
    output_dir = _OUTPUT_DIR_BASE + '.%d' % index_free

sc = SparkContext( appName="analyze-mmapplypolicy" )

lines = sc.textFile( name=_INPUT_FILE ).coalesce(_NUM_PARTITIONS)

def parse_mmapplypolicy_line( line ):
    columns = line.strip().split()
    filename = columns[1].replace('%2F','/')
    if filename.endswith('.gz') or filename.endswith('.bz2'):
        fileext = '.'.join( filename.split('.')[-2:] )
    else:
        fileext = filename.split('.')[-1]
    filesize = int(columns[0])
    return [ ( fileext, filesize ) ]

keyvals = lines.flatMap( lambda line: parse_mmapplypolicy_line( line ) )
counts = keyvals.reduceByKey( lambda a, b: a + b )
counts.sortByKey().coalesce(1).saveAsTextFile( output_dir )
