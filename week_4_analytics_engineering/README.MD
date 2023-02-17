# Prerequisites
## Upload dataset CSV files to GCS Bucket:
We need the following datasets:
- Yellow taxi data - Years 2019 and 2020
- Green taxi data - Years 2019 and 2020
- fhv data - Year 2019

To upload all of them to GCS bucket we have to run the script ``1_homework/load_dataset_files_to_gcs.py``

Also we will need the dbt models, schema and macros. You can download them from bootcamp repository:
- [dbt macros](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_4_analytics_engineering/taxi_rides_ny/macros)
- [dbt staging models and schema](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_4_analytics_engineering/taxi_rides_ny/models/staging)
