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
#                 --driver-memory=15G \
#                 --executor-memory=50G \
#                 ./analyze-mmapplypolicy.py
#    stop-all.sh
#

from pyspark import SparkContext

sc = SparkContext( appName="analyze-mmapplypolicy" )

# lines = sc.textFile( '/global/cscratch1/sd/glock/mmapplypolicy.sample.gz' )
lines = sc.textFile( '/scratch1/scratchdirs/glock/mmapplypolicy.out.gz' )

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
counts.sortByKey().saveAsTextFile( '/scratch1/scratchdirs/glock/mmapplypolicy.sparked' )
