

module load python/gnu/3.4.4
module load spark/2.2.0
export PYSPARK_PYTHON='/share/apps/python/3.4.4/bin/python'
export PYSPARK_DRIVER_PYTHON='/share/apps/python/3.4.4/bin/python'



SPARKCODE=$(echo "$2/task$1".py)
DIFFFILE="results/task$1.diff"
TMPFILE="$2/task$1tmp.out"
if [ -e "$DIFFFILE" ]; then
	rm "$DIFFFILE" 
fi

/usr/bin/hadoop fs -rm -r "task$1.out"

if [ "$1" -eq 1 ]
then
	spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python "$SPARKCODE" /user/ecc290/HW1data/parking-violations.csv /user/ecc290/HW1data/open-violations.csv
	/usr/bin/hadoop fs -getmerge "task$1.out" "$TMPFILE".tmp
	cat "$TMPFILE".tmp | sort -n > "$TMPFILE"
	rm "$TMPFILE".tmp
elif [ "$1" -eq 6 ]
then
	spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python "$SPARKCODE" /user/ecc290/HW1data/parking-violations.csv
	/usr/bin/hadoop fs -getmerge "task$1.out" "$TMPFILE"
elif [ "$1" -eq 7 ]
then
	spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python "$SPARKCODE" /user/ecc290/HW1data/parking-violations.csv
	/usr/bin/hadoop fs -getmerge "task$1.out" "$TMPFILE".tmp
	cat "$TMPFILE".tmp | python check7.py | sort -n > "$TMPFILE"
	rm "$TMPFILE".tmp
elif [ "$1" -eq 3 ]
then
	spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python "$SPARKCODE" /user/ecc290/HW1data/open-violations.csv
	/usr/bin/hadoop fs -getmerge "task$1.out" "$TMPFILE".tmp
	cat "$TMPFILE".tmp | sort -n > "$TMPFILE"
	rm "$TMPFILE".tmp
	
else
	spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python "$SPARKCODE" /user/ecc290/HW1data/parking-violations.csv
	/usr/bin/hadoop fs -getmerge "task$1.out" "$TMPFILE".tmp
	cat "$TMPFILE".tmp | sort -n > "$TMPFILE"
	rm "$TMPFILE".tmp
	
fi

if [ -e "$TMPFILE" ]
then
	if [ "$1" -eq 7 ]
	then
		DIFF=$(diff -w "keys/task$1-trunc.out" "$TMPFILE")
		DIFFC=$(diff -w -y "keys/task$1-trunc.out" "$TMPFILE")
	else
		DIFF=$(diff -w "keys/task$1.out" "$TMPFILE")
		DIFFC=$(diff -w -y "keys/task$1.out" "$TMPFILE")
	fi
	if [ "$DIFF" ]
	then 
		if [ ! -d "results" ]
		then
			mkdir results
		fi
		echo "$DIFFC" > "results/task$1.diff"
		echo "Task $1: Failed. See errors in results/task$1.diff"
	else
                echo "Task $1: Passed."
	fi
else
	echo "Task $1: Failed; no output generated."
fi
rm -f "$TMPFILE"  


