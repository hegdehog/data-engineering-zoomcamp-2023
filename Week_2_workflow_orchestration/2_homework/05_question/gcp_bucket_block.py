from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket


bucket_block = GcsBucket(
    gcp_credentials=GcpCredentials.load("zoom-gcp-creds"),
    bucket="prefect-de-zoomcamp",
)

bucket_block.save("zoom-gcs", overwrite=True)