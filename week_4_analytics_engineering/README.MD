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


# Exercises
We will use the data loaded for:

- Building a source table: stg_fhv_tripdata
- Building a fact table: fact_fhv_trips
- Create a dashboard

## stg_fhv_tripdata
Once we have in BigQuery a fhv table with data from 2019 we have to create the `dbt model`:

1. Update `/models/staging/schema.yml` with the source table and new model:
    
          tables:
            - name: green_tripdata
            - name: yellow_tripdata
            - name: fhv_tripdata
            
          models:
            - name: stg_fhv_tripdata
            
2. Create a model called `stg_fhv_tripdata.sql` on /models/

    ```sql
    {{ config(materialized='view') }}


    with tripdata as 
    (
      select *, right(dispatching_base_num,5) as dispatching_base_num_int,
        row_number() over(partition by dispatching_base_num, pickup_datetime) as rn
      from {{ source('staging','fhv_tripdata') }}
      where dispatching_base_num is not null 
    )
    select
        -- identifiers
        {{ dbt_utils.surrogate_key(['dispatching_base_num_int', 'pickup_datetime']) }} as tripid,
        cast(dispatching_base_num_int as integer) as dispatching_base_num,
        cast(PULocationID as integer) as PULocationID,
        cast(DOLocationID as integer) as DOLocationID,

        -- timestamps
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropoff_datetime as timestamp) as dropoff_datetime,

        -- trip info
        SR_Flag,
        Affiliated_base_number
    from tripdata
    where rn = 1

    -- dbt build --m <model.sql> --var 'is_test_run: false'
    {% if var('is_test_run', default=true) %}

      limit 100

    {% endif %}```
    
3. Run command `dbt build`

    ```dbt build --select stg_fhv_tripdata.sql --var 'is_test_run: false'```
    
4. Create the fact table crossing FHV data with taxi zones lookup dataset `fact_table_fhv.sql` on /models/core:

    ```sql
    {{ config(materialized='table') }}

    with fhv_data as (
        select *, 
            'Fhv' as service_type 
        from {{ ref('stg_fhv_tripdata') }}
    ),

    dim_zones as (
        select * from {{ ref('taxi_zone_lookup') }}
        where borough != 'Unknown'
    )
    select 
        fhv_data.*,
        pickup_zone.borough as pickup_borough, 
        pickup_zone.zone as pickup_zone, 
        dropoff_zone.borough as dropoff_borough, 
        dropoff_zone.zone as dropoff_zone,  
    from fhv_data
    inner join dim_zones as pickup_zone
    on fhv_data.PULocationID = pickup_zone.locationid
    inner join dim_zones as dropoff_zone
    on fhv_data.DOLocationID = dropoff_zone.locationid```
      
5. Run command `dbt build`

    ```dbt build --select fact_table_fhv.sql --var 'is_test_run: false'```
    
6. Check how many rows has the new table fact_table_fhv with all dependencies:

    ```sql 
        select count(1)
        from `digital-aloe-375022`.`dbt_mtorregrosa`.`fact_table_fhv`
    ```
    
    ![image](https://user-images.githubusercontent.com/19264618/220082483-766269fd-db11-4079-9687-97dbdc7bbe19.png)
    
7. Group by month and check the month with the biggest amount of rides
    
    ```sql
    select EXTRACT(MONTH from pickup_datetime), count(1)
    from `digital-aloe-375022`.`dbt_mtorregrosa`.`fact_table_fhv`
    GROUP BY EXTRACT(MONTH from pickup_datetime)
    ORDER BY 2 DESC
    ```
    
    ![image](https://user-images.githubusercontent.com/19264618/220082839-4911040f-a7b5-4153-8d42-8aa24b0844c2.png)

