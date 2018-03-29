#!/usr/bin/env python
import sys
from pyspark.sql import *
from pyspark.sql.functions import *

def main():
    spark = SparkSession.builder.appName("sql task3").config("spark.some.config.option", "some-value").getOrCreate()
    open_violation = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
    open_violation.createOrReplaceTempView("open")
    result = spark.sql("SELECT license_type, SUM(amount_due) AS total, AVG(amount_due) AS average\
                        FROM open \
                        GROUP BY license_type")
    result.select(format_string("%s\t%.2f, %.2f",result.license_type, result.total, result.average)).write.save("task3-sql.out",format="text")


if __name__ == "__main__":
    main()

