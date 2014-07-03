#!/usr/bin/env pyspark
################################################################################
#  A simple wordcount example for Spark, designed to run on SDSC's Gordon
#  resource.
#
#  Glenn K. Lockwood, San Diego Supercomputer Center                 July 2014
################################################################################

from pyspark import SparkContext

### We need to figure out our Spark master and HDFS namenode
import os
spark_master_uri = "spark://%s:%s" % (  os.environ['SPARK_MASTER_IP'], 
                                        os.environ['SPARK_MASTER_PORT'] )

### Here we assume that the HDFS namenode is running on the same host
### as our Spark master
my_hdfs_dir =  "hdfs://%s:%d/user/%s/" % ( os.environ['SPARK_MASTER_IP'], 
                                           54310,
                                           os.environ['USER'] )

### If you want to try this example using the interactive pyspark shell, 
### sc will already exist based on your environemtn and the following
### call to SparkContext() is not necessary
sc = SparkContext( spark_master_uri, "Wordcount Test")

### Do an actual word count ####################################################
### Create an RDD from a file residing on HDFS
file = sc.textFile(my_hdfs_dir + 'gutenberg.txt')

### flatMap() can transform one element into zero or multiple elements
words = file.flatMap(lambda line: line.split())

### map() transforms every RDD element into a single new element
keyvals = words.map(lambda word: (word, 1))

### Given an RDD containing (key, value) tuples, return an RDD containing 
### unique keys and a single reduced value for each
counts = keyvals.reduceByKey(lambda a, b: a + b)

### Save our RDD as a text file back to HDFS
counts.saveAsTextFile(my_hdfs_dir + 'output.dir')
