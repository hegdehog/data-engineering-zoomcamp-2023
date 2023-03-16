import pyspark.sql.types as types

INPUT_DATA_PATH = '../../resources/rides.csv'
BOOTSTRAP_SERVERS = 'localhost:9092'

TOPIC_WINDOWED_VENDOR_ID_COUNT = 'vendor_counts_windowed'

PRODUCE_TOPIC_RIDES_CSV = CONSUME_TOPIC_RIDES_CSV = 'rides_csv'

RIDE_SCHEMA = types.StructType([
    types.StructField("dispatching_base_num", types.StringType(), True),
    types.StructField("pickup_datetime", types.TimestampType(), True),
    types.StructField("dropoff_datetime", types.TimestampType(), True),
    types.StructField("PULocationID", types.LongType(), True),
    types.StructField("DOLocationID", types.LongType(), True),
    types.StructField("SR_Flag", types.StringType(), True),
    types.StructField("Affiliated_base_number", types.StringType(), True)
])

