from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color, year, month) -> Path:
    """Data Extract from Google Cloud Storage"""
    gcs_path = f"{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"./data/")
    return Path(f"./data/{gcs_path}")

@task()
def transform(path) -> pd.DataFrame:
    """Data Cleaning"""
    df = pd.read_parquet(path)
    # print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    # df['passenger_count'].fillna(0, inplace=True)
    # print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df

@task(log_stdout=True)
def write_bq(df) -> None:
    """Write DataFrame to BigQuery"""
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="digital-aloe-375022",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000, 
        if_exists="append"
    )

@flow()
def etl_gcs_to_bq(year, month, color):

    path = extract_from_gcs(color, year, month)
    df = transform(path)
    numRowsProcessed = df.shape[0]
    print(f"Rows processed: {numRowsProcessed}")
    write_bq(df)


@flow()
def etl_parent_flow(color: str = "yellow", months: list[int] = [1,2], year: int = 2021):
    for month in months:
        etl_gcs_to_bq(year, month, color)
    

if __name__ == "__main__":
    color="yellow"
    months=[2,3]
    year=2019
    etl_parent_flow(color,months, year)
