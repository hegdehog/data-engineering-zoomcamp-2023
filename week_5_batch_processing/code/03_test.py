import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName('test2') \
    .getOrCreate()

!wget https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2021-01.parquet

df = spark.read \
    .parquet('fhvhv_tripdata_2021-01.parquet')

df = df.repartition(24)
df.write.parquet('data/fhvhv/2021/01/')