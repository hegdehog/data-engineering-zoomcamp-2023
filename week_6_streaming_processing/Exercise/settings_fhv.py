import pyspark.sql.types as T

INPUT_DATA_PATH = 'resources/fhv_tripdata_2019-01.csv'
BOOTSTRAP_SERVERS = 'localhost:9092'

TOPIC_WINDOWED_VENDOR_ID_COUNT = 'vendor_counts_windowed'

PRODUCE_TOPIC_RIDES_CSV = CONSUME_TOPIC_RIDES_CSV = 'fhv_csv'

RIDE_SCHEMA = T.StructType(
    [T.StructField("dispatching_base_num", T.StringType()),
     T.StructField('pickup_datetime', T.TimestampType()),
     T.StructField('dropOff_datetime', T.TimestampType()),
     T.StructField("PUlocationID", T.IntegerType()),
     T.StructField("DOlocationID", T.FloatType()),
     T.StructField("SR_Flag", T.IntegerType()),
     T.StructField("Affiliated_base_number", T.FloatType()),
     ])
