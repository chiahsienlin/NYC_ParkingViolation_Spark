#!/usr/bin/env python
import sys
from pyspark import SparkContext
from csv import reader
def ComputeTotal(x,y):
    total = float(x[0]) + float(y[0])
    nums = x[1] + y[1]
    return (total, nums)
def FormatOutput(x):
    avg = float(x[1][0])/float(x[1][1])
    return x[0] + "\t" + "{0:.2f}".format(float(x[1][0])) + ", " + "{0:.2f}".format(avg)
def main():
   sc = SparkContext()
   rdd = sc.textFile(sys.argv[1])
   rdd = rdd.mapPartitions(lambda x: reader(x))
   rdd = rdd.map(lambda x: (x[2], (x[12], 1.0)))
   rdd = rdd.reduceByKey(ComputeTotal).map(FormatOutput)
   rdd.saveAsTextFile("task3.out")

if __name__ == "__main__":
    main()
