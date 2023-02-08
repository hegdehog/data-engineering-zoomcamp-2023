# Exercises
## Question 1. What is count for fhv vehicle records for year 2019?

1. Copy flow ``flow etl_web_to_gcs.py`` and modify it to download fhv files from github (https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv) and load them as ``.csv.gz`` to my Google Cloud Storage Bucket.
2. Create external table from all csv.gz files. Make sure first that your BigQuery has access to your Google Cloud Storage Bucket on IAM. NOTE: remember you have to place BG and GCS in the same region.

		CREATE OR REPLACE EXTERNAL TABLE region6.fhv_table_EXT
        OPTIONS (
        format = 'CSV',
        uris = ['gs://dtc_data_lake_digital-aloe-375022/fhv*']
        );
        
3. Persist data on BQ Table:        
    
		CREATE TABLE  digital-aloe-375022.region6.fhv_table
		AS SELECT * FROM digital-aloe-375022.region6.fhv_table_EXT

4. Run count query on the external table (or BQ table, it's the same):

		SELECT Count(1) FROM digital-aloe-375022.region6.fhv_table_EXT
        
## Question 2. estimated amount of data

1. Write query and before run, check the estimated cost on BQ Console:

		SELECT count(distinct affiliated_base_number ) 
		FROM digital-aloe-375022.region6.fhv_table
		  
## Question 4. Partition and cluster

1. Run query to create a partitioned and clustered new table:

		CREATE OR REPLACE TABLE `digital-aloe-375022.region6.fhv_table_partitioned`
		PARTITION BY DATE( pickup_datetime )
		CLUSTER BY affiliated_base_number AS
		SELECT * FROM `digital-aloe-375022.region6.fhv_table`

## Question 3. How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?

1. Run query:

		SELECT count(1) 
		FROM digital-aloe-375022.region6.fhv_table
        where PUlocationID is null 
        and DOlocationID is null
        
        
## Question 5. Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 03/01/2019 and 03/31/2019 (inclusive)

1.  Write query and before run, check the estimated cost on BQ Console:

		SELECT distinct affiliated_base_number FROM `digital-aloe-375022.region6.fhv_table`
		where pickup_datetime >= '2019-03-01' and pickup_datetime <= '2019-03-31'
