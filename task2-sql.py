#!/usr/bin/env python
import sys
from pyspark.sql import *
from pyspark.sql.functions import *

def main():
    spark = SparkSession.builder.appName("sql task2").config("spark.some.config.option", "some-value").getOrCreate()

    park_violation = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
    park_violation.createOrReplaceTempView('park')
    
    result = spark.sql("SELECT violation_code, COUNT(summons_number) AS total\
                       FROM park\
                       GROUP BY violation_code")
    result.select(format_string("%d\t%d",result.violation_code, result.total)).write.save("task2-sql.out",format="text")
if __name__ == "__main__":
    main()
