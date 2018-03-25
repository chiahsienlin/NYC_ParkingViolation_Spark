#!/usr/bin/env python
import sys
from pyspark import SparkContext
from csv import reader
def MapFunc(x):
    code = x[2]
    date = x[1]
    day = int (date.split("-")[-1])
    if (day % 7 == 5) or (day % 7 == 6):
        return ((int(code),"weekend"),1)
    else:
        return ((int(code),"weekday"),1)
def SecMapFunc(x):
    if x[0][1] == "weekend":
        return (x[0][0], ("weekend", x[1]))
    else:
        return (x[0][0], ("weekday", x[1]))
def ComputeAvg(x,y):
    if x[0] == "weekend":
       avg_weekend = float(x[1])/8.0
       avg_week = float(y[1])/23.0
    else:
        avg_weekend = float(y[1])/8.0
        avg_week = float(x[1])/23.0
    return avg_weekend, avg_week
def isFloat(x):
    try:
        float(x)
        return True
    except ValueError:
        return False
def FormatOutput(x):
    if isFloat(x[1][0]):
        return str(x[0]) + "\t" + "{0:.2f}".format(x[1][0]) + ", " + "{0:.2f}".format(x[1][1])
    else:
        if x[1][0] == "weekend":
            return str(x[0]) + "\t" + "{0:.2f}".format(x[1][1]/8.0) + ", " + "0.00" 
        else:
            return str(x[0]) + "\t" + "0.00" + ", " + "{0:.2f}".format(x[1][1]/23.0)
def main():
    sc = SparkContext()
    rdd = sc.textFile(sys.argv[1])
    rdd = rdd.mapPartitions(lambda x: reader(x))
    rdd = rdd.map(MapFunc).reduceByKey(lambda x, y: x+y).map(SecMapFunc).reduceByKey(ComputeAvg).map(FormatOutput)
    rdd.saveAsTextFile("task7.out")
    sc.stop()

if __name__ == "__main__":
    main()
