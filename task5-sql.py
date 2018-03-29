#!/usr/bin/env python
import sys
from pyspark.sql import *
from pyspark.sql.functions import *

def main():
    spark = SparkSession.builder.appName("sql task5").config("spark.some.config.option", "some-value").getOrCreate()
    park_violation = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
    park_violation.createOrReplaceTempView("park")
    result = spark.sql("SELECT plate_id, registration_state, COUNT(summons_number) AS cnt\
               FROM park \
               GROUP BY plate_id, registration_state\
               ORDER BY cnt DESC\
               LIMIT 1")
    result.select(format_string("%s, %s\t%d",result.plate_id,result.registration_state, result.cnt)).write.save("task5-sql.out",format="text")

if __name__ == "__main__":
    main()
