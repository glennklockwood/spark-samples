Spark Samples
=============

This represents the beginning of a tutorial I was creating for Spark users on 
the Gordon supercomputer at the San Diego Supercomputer Center.  I ran out of 
time when building out the full set of training material, so now I just keep
some Spark scripts I've written and used.

wordcount-spark.py
------------------
This is the canonical word count example implemented in pyspark.

analyze-mmapplypolicy.py
------------------------
This is a script to perform some analysis of the output of the `mmapplypolicy`
command that dumps every file and most of its metadata on a GPFS cluster.  As
of July 5, 2016, this only works on a pre-filtered format and not the full
`mmapplypolicy` output.
