#!/usr/bin/env python
import sys
from pyspark.sql import *
from pyspark.sql.functions import *

def main():
    spark = SparkSession.builder.appName("sql task1").config("spark.some.config.option", "some-value").getOrCreate()
    park_violation = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
    open_violation = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[2])
    park_violation.createOrReplaceTempView("park")
    open_violation.createOrReplaceTempView("open")
    spark.sql("SELECT P.summons_number, P.plate_id, P.violation_precinct, P.violation_code, P.issue_date \
               FROM park P INNER JOIN open OP \
               ON P.summons_number = OP.summons_number").createOrReplaceTempView("tmp")
    result = spark.sql("(SELECT P.summons_number, P.plate_id, P.violation_precinct, P.violation_code, P.issue_date \
                FROM park P) \
                EXCEPT \
                SELECT * FROM tmp")
    result.select(format_string("%d\t%s, %d, %d, %s",result.summons_number, result.plate_id, result.violation_precinct, result.violation_code, date_format(result.issue_date,'yyyy-MM-dd'))).write.save("task1-sql.out",format="text")

if __name__ == "__main__":
    main()
