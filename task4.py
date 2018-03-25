#!/usr/bin/env python
import sys
from pyspark import SparkContext
from csv import reader
def MapFunc(x):
    if x[16] == "NY":
        return (x[16], 1)
    else:
        return ("Other", 1)
def OutputFormat(x):
    return str(x[0])+ "\t" + str(x[1])
def main():
    sc = SparkContext()
    rdd = sc.textFile(sys.argv[1])
    rdd = rdd.mapPartitions(lambda x: reader(x))
    rdd = rdd.map(MapFunc).reduceByKey(lambda x, y: x+y).map(OutputFormat)
    rdd.saveAsTextFile("task4.out")
    #print(rdd.collect())
if __name__ == "__main__":
    main()
