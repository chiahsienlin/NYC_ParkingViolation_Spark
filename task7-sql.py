#!/usr/bin/env python
import sys
from pyspark.sql import *
from pyspark.sql.functions import *

def main():
    spark = SparkSession.builder.appName("sql task6").config("spark.some.config.option", "some-value").getOrCreate()
    park_violation = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
    park_violation.createOrReplaceTempView("park")
    result = spark.sql("SELECT B.violation_code, A.weekend, B.allcnt \
                        FROM (SELECT violation_code, COUNT(summons_number) AS weekend\
                              FROM park\
                              WHERE DAY(issue_date)%7 = 5 OR DAY(issue_date)%7 = 6\
                              GROUP BY violation_code) AS A\
                              RIGHT OUTER JOIN \
                             (SELECT violation_code, COUNT(summons_number) AS allcnt\
                              FROM park\
                              GROUP BY violation_code) AS B\
                              ON A.violation_code = B.violation_code\
                        ORDER BY B.violation_code")
    result = result.fillna(0)
    result.select(format_string("%d\t%.2f, %.2f",result.violation_code,result.weekend*1.0/8, (result.allcnt - result.weekend)*1.0 /23)).write.save("task7-sql.out",format="text")


if __name__ == "__main__":
    main()
