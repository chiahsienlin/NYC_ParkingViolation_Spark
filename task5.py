#!/usr/bin/env python
import sys
from pyspark import SparkContext
from csv import reader

def FormatOutput(x):
    return str(x[0][0])+ ", " + str(x[0][1]) + "\t" + str(x[1])

def main():
    sc = SparkContext()
    rdd = sc.textFile(sys.argv[1])
    rdd = rdd.mapPartitions(lambda x: reader(x))
    rdd = rdd.map(lambda x: ((str(x[14]), str(x[16])), 1))
    rdd = rdd.reduceByKey(lambda x, y: x+y).takeOrdered(1, key = lambda x: -x[1])
    rdd = sc.parallelize(rdd)
    rdd = rdd.map(FormatOutput)
    rdd.saveAsTextFile("task5.out")
    sc.stop()

if __name__ == "__main__":
    main()
