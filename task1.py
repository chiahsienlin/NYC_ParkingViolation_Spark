#!/usr/bin/env python
import sys
from pyspark import SparkContext
from csv import reader

def CombineTuple(x, y):
    flag = x[4] + y[4]
    if x[4] == 1:
        return (x[0],x[1],x[2],x[3],flag)
    else:
        return (y[0],y[1],y[2],y[3],flag)
def TuplewithOne(x):
    return x[1][4] > 0
def FormatOutput(x):
    return x[0] + "\t" + x[1][0] + ", "+ x[1][1] + ", " + x[1][2] + ", "+ x[1][3]
def main():
    sc = SparkContext()
    #read csv
    rdd_park = sc.textFile(sys.argv[1])
    rdd_open = sc.textFile(sys.argv[2])
    rdd_park = rdd_park.mapPartitions(lambda x : reader(x))
    rdd_open = rdd_open.mapPartitions(lambda x : reader(x))
    #map
    rdd_park = rdd_park.map(lambda x: (x[0], (x[14], x[6], x[2], x[1], 1)))
    rdd_open = rdd_open.map(lambda x: (x[0], ("v", "v", "v", "v", -1)))
    rdd_all = rdd_park.union(rdd_open)
    #reduce
    rdd_all = rdd_all.reduceByKey(CombineTuple).filter(TuplewithOne).map(FormatOutput)
    #output
    rdd_all.saveAsTextFile("task1.out")
    sc.stop()

if __name__ == "__main__":
    main()
