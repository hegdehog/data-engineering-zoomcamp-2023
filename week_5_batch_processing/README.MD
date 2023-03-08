# Exercises
## Question 1: Run the command "spark.version" What version number is output?
1. Update `/models/staging/schema.yml` with the source table and new model:

```python
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()
    
spark.version
 ```

## Question 2: Repartition the June 2021 HVFHV Data into 12 partitions and save it to Parquet. What is the average size of the Parquet Files?
1. Download June 2021 HVFHV file from https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhvhv/fhvhv_tripdata_2021-06.csv.gz

2. Load parqruet into dataframe to get schema

```python
df = spark.read \
    .option("header", "true") \
    .csv("fhv_tripdata_2021-06.csv")
```

    And get this:

```python
StructType([StructField('dispatching_base_num', StringType(), True), StructField('pickup_datetime', StringType(), True), 
StructField('dropoff_datetime', StringType(), True), StructField('PULocationID', StringType(), True), 
StructField('DOLocationID', StringType(), True), StructField('SR_Flag', StringType(), True), StructField('Affiliated_base_number', StringType(), True)])
```

3. Which we transform into: 

```python
fhv_schema = types.StructType([
    types.StructField("dispatching_base_num", types.StringType(), True),
    types.StructField("pickup_datetime", types.TimestampType(), True),
    types.StructField("dropoff_datetime", types.TimestampType(), True),
    types.StructField("PULocationID", types.LongType(), True),
    types.StructField("DOLocationID", types.LongType(), True),
    types.StructField("SR_Flag", types.StringType(), True),
    types.StructField("Affiliated_base_number", types.StringType(), True)
])
```

4. Write a parquet partitioned in 12 files:

```python
df_fhv \
    .repartition(12) \
    .write.parquet("data")
```




## Question 3: Count records - How many taxi trips were there on June 15? Consider only trips that started on June 15.
We can calculate it using Spark SQL:

```python
df_fhv.createOrReplaceTempView("df_fhv")

df_sql = spark.sql("""
SELECT 
    COUNT(1) AS number_records
FROM
    df_fhv
WHERE date_trunc('day', pickup_datetime) == "2021-06-15"
""")

df_sql.show()
``` 
Or PySpark:

```python
from pyspark.sql import functions as F

df_fhv_15_jun = df_fhv \
    .filter(F.date_trunc("day", "pickup_datetime") == "2021-06-15") 

df_fhv_15_jun.count()
```


## Question 4: Longest trip for each day. Now calculate the duration for each trip. How long was the longest trip in Hours?
We can calculate it using Spark SQL:

```python
df_longest = spark.sql("""
SELECT 
    pickup_datetime, dropoff_datetime,
    (unix_timestamp(dropoff_datetime)-unix_timestamp(pickup_datetime))/(3600) as diff 
FROM
    df_fhv
    order by 3 desc
""")

df_longest.show()
```
        
Or PySpark:

```python
from pyspark.sql import functions as F

df_fhv_15_jun = df_fhv \
    .withColumn("diff", (unix_timestamp("dropoff_datetime")-unix_timestamp("pickup_datetime"))/3600) \
    .orderBy(col("diff").desc())

df_fhv_15_jun.select("pickup_datetime","dropoff_datetime","diff").show(10)
```

# Question 6: What is the name of the most frequent pickup location zone?
1. Firstly we load the lookup file with zones:

```python
df_zones = spark.read \
    .option("header", "true") \
    .csv('../code/taxi+_zone_lookup.csv')
```

2. Rename the column `LocationID` to `PULocationID` in order to join with other DataFrame

```python
df_zones = df_zones.withColumnRenamed('LocationID', 'PULocationID') 
```

3. Join both DataFrames

```python
df_join = df_fhv.join(df_zones.select("PULocationID","Zone"), on=['PULocationID'], how='inner')
```

5. Run Spark SQL:

```python
df_join.createOrReplaceTempView("df_join")

df_freq_pu_zones = spark.sql("""
SELECT 
    count(1), zone
FROM
    df_join
GROUP BY zone
ORDER BY 1 DESC
""")

df_freq_pu_zones.show(10)
```

Or PySpark:

```python
df_join \
.groupBy("zone") \
.agg({ "*": "count"}) \
.withColumnRenamed("count(1)", "amount") \
.orderBy(col("amount"), ascending=False) \
.show()
```
