from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import requests
import os

@task()
def export_data_local(type, dataset_file, dataset_url):
    if not os.path.exists(f"data/{type}"):
        os.makedirs(f"data/{type}")
   
    r = requests.get(dataset_url, allow_redirects=True)

    open(f"data/{type}/{dataset_file}", 'wb').write(r.content)


@task(log_prints=True)
def export_data_gcs( type, path ):
    path = f"data/{type}/{path}"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    

@flow()
def etl_web_to_gcs(year, type, month):
    dataset_file = f"{type}_tripdata_{year}-{month:02}.csv.gz"
    print (dataset_file)
    dataset_url  = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{type}/{dataset_file}"
    export_data_local(type, dataset_file, dataset_url)
    export_data_gcs(type, dataset_file)


@flow()
def etl_parent_flow():
    types = {"green", "yellow", "fhv"}
    years = {"2019", "2020"}

    for year in years:
        for type in types:
            if year == 2019 and type == "fhv": 
                exit
            else:
                for x in range(12):
                    etl_web_to_gcs(year,  type, x+1)
        

if __name__ == "__main__":
    etl_parent_flow()
