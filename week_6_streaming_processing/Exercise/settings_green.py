import pyspark.sql.types as T

INPUT_DATA_PATH = 'resources/green_tripdata_2019-01.csv'
BOOTSTRAP_SERVERS = 'localhost:9092'

TOPIC_WINDOWED_VENDOR_ID_COUNT = 'vendor_counts_windowed'

PRODUCE_TOPIC_RIDES_CSV = CONSUME_TOPIC_RIDES_CSV = 'green_csv'

RIDE_SCHEMA = T.StructType(
    [
    T.StructField('VendorID', T.StringType(), ), 
    T.StructField('lpep_pickup_datetime', T.TimestampType(), ), 
    T.StructField('lpep_dropoff_datetime', T.TimestampType(), ), 
    T.StructField('store_and_fwd_flag', T.StringType(), ), 
    T.StructField('RatecodeID', T.IntegerType(), ), 
    T.StructField('PULocationID',T.IntegerType(), ), 
    T.StructField('DOLocationID', T.IntegerType(), ), 
    T.StructField('passenger_count', T.FloatType(), ), 
    T.StructField('trip_distance', T.StringType(), ), 
    T.StructField('fare_amount', T.FloatType(), ), 
    T.StructField('extra', T.FloatType(), ), 
    T.StructField('mta_tax', T.FloatType(), ), 
    T.StructField('tip_amount', T.FloatType(), ), 
    T.StructField('tolls_amount', T.StringType(), ),
    T.StructField('ehail_fee', T.StringType(), ), 
    T.StructField('improvement_surcharge', T.StringType(), ),
    T.StructField('total_amount', T.StringType(), ), 
    T.StructField('payment_type', T.StringType(), ), 
    T.StructField('trip_type', T.StringType(), ), 
    T.StructField('congestion_surcharge', T.StringType(), )
])