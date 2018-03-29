#!/usr/bin/env python
import sys
from pyspark.sql import *
from pyspark.sql.functions import *

def main():
    spark = SparkSession.builder.appName("sql task4").config("spark.some.config.option", "some-value").getOrCreate()
    park_violation = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
    park_violation.createOrReplaceTempView("park") 
    result = spark.sql("SELECT all_cnt, registration_state, COUNT(summons_number) AS state_cnt\
                        FROM  park  CROSS JOIN (SELECT COUNT(summons_number) AS all_cnt FROM park)\
                        GROUP BY registration_state, all_cnt\
                        HAVING registration_state = 'NY'")
    result.select(format_string("%s\t%d\nOther\t%d",result.registration_state, result.state_cnt, result.all_cnt - result.state_cnt)).write.save("task4-sql.out",format="text")


if __name__ == "__main__":
    main()

