# Submit Python code to SparkMaster

if [ $# -lt 1 ]
then
	echo "Usage: $0 <pyspark-job.py> [ executor-memory ]"
	echo "(specify memory in string format such as \"512M\" or \"2G\")"
	exit 1
fi
z<z<z<ON_JOB=$1

if [ -z $2 ]
then
	EXEC_MEM="1G"
else
	EXEC_MEM=$2
fi
spark-submit.cmd --master spark://localhost:7077 --num-executors 2 --executor-memory 512M --executor-cores 1 streaming.py
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.apache.spark:spark-avro_2.12:3.3.1,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.1 \ streaming.py