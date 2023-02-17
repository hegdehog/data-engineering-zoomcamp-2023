from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import requests


@task()
def export_data_local(dataset_file, dataset_url):
    r = requests.get(dataset_url, allow_redirects=True)

    open(f"fhv/{dataset_file}", 'wb').write(r.content)


@task(log_prints=True)
def export_data_gcs( path ):
    path = f"fhv/{path}"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    

@flow()
def etl_web_to_gcs(year, month):
    dataset_file = f"fhv_tripdata_{year}-{month:02}.csv.gz"
    print (dataset_file)
    dataset_url  = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}"
    export_data_local(dataset_file, dataset_url)
    export_data_gcs(dataset_file)


@flow()
def etl_parent_flow():
    year = 2019
    for x in range(12):
        etl_web_to_gcs(year, x+1)
    

if __name__ == "__main__":
    etl_parent_flow()
