#!/usr/bin/env python
import sys
from pyspark import SparkContext
from csv import reader
def FormatOutput(x):
    return str(x[0])+ "\t" + str(x[1])

def main():
    sc = SparkContext()
    rdd = sc.textFile(sys.argv[1])
    rdd = rdd.mapPartitions(lambda x: reader(x))
    rdd = rdd.map(lambda x: (int(x[2]), 1))
    rdd = rdd.reduceByKey(lambda x, y: x+y).sortByKey().map(FormatOutput)
    rdd.saveAsTextFile("task2.out")
    sc.stop()
if __name__ == "__main__":
    main()

