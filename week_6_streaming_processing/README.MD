# Exercise
Please implement a streaming application, for finding out popularity of PUlocationID across green and fhv trip datasets.
Please use the datasets fhv_tripdata_2019-01.csv.gz
and green_tripdata_2019-01.csv.gz

Firstly we have to create the producer and consumer scripts for both sources. We clone the scripts, change the schemas and run them:


1. Create the abbreviate schemas for both files (we only need the PULocationID, so we get this field from the stream and a few more for checking):

```python
fhv_schema = T.StructType(
    [T.StructField("dispatching_base_num", T.StringType()),
     T.StructField('pickup_datetime', T.TimestampType()),
     T.StructField('dropOff_datetime', T.TimestampType()),
     T.StructField("PUlocationID", T.IntegerType()),
     T.StructField("DOlocationID", T.FloatType()),
     T.StructField("Affiliated_base_number", T.FloatType()),
     ])

green_schema = T.StructType(
    [T.StructField('VendorID', T.StringType(), ), 
    T.StructField('lpep_pickup_datetime', T.TimestampType(), ), 
    T.StructField('lpep_dropoff_datetime', T.TimestampType(), ), 
    T.StructField('store_and_fwd_flag', T.StringType(), ), 
    T.StructField('RatecodeID', T.IntegerType(), ), 
    T.StructField('PULocationID',T.IntegerType(), ), 
    T.StructField('DOLocationID', T.IntegerType(), )
])
```

2. We create the two readStrem to connect to both topics

```python
    df_kafka_raw_fhv = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092,broker:29092") \
        .option("subscribe", "fhv_csv") \
        .option("startingOffsets", "earliest") \
        .option("checkpointLocation", "checkpoint") \
        .load()

    df_kafka_raw_green = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092,broker:29092") \
        .option("subscribe", "green_csv") \
        .option("startingOffsets", "earliest") \
        .option("checkpointLocation", "checkpoint") \
        .load()
```


3. We parse the message's values from both topics:

```python
        df_rides_fhv = parse_ride_from_kafka_message(df_raw=df_kafka_raw_fhv, schema=fhv_schema)
        df_rides_green = parse_ride_from_kafka_message(df_raw=df_kafka_raw_green, schema=green_schema)
```

4. Save the values on dataframes

```python
        query_name = 'q_fhv3'
        query_template = 'select PUlocationID from {table_name}'
        write_query, df_popular_pulocation_fhv = sink_memory(df=df_rides_fhv, query_name=query_name, query_template=query_template)

        query_name = 'q_green3'
        query_template = 'select PULocationID from {table_name}'
        write_query, df_popular_pulocation_green = sink_memory(df=df_rides_green, query_name=query_name, query_template=query_template)
```

5. And finally combine both dataframes and calculate the most popular PULocationID from the messages received:

```python
        from pyspark.sql.functions import col
        df_total.groupBy('PUlocationID') \
            .count() \
            .sort(col("count").desc()) \
            .show()
```