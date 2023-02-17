# Exercises
## Question 1. Load January 2020 data
1. Copy flow ``flow etl_web_to_gcs.py`` and modify params on ``__main__``:

        if __name__ == "__main__":
                color="green"
                month=[1]
                year=2020
                etl_web_to_gcs(year, color, month)

## Question 2. Scheduling with Cron
We can do it using the Prefect GUI or building the deployment yaml file and then adding cron schedule rule: ``0 5 1 * *``

        schedule: 
        cron: 0 5 1 * *
        timezone: Europe/Madrid

## Question 3. Loading data to BigQuery
1. Load into GCS Yellow taxi data for Feb. 2019 and March 2019. I have copied the ``flow etl_web_to_gcs.py`` and added a parent flow to process both months.

        @flow()
        def etl_parent_flow(color: str = "yellow", months: list[int] = [1,2], year: int = 2021):
        for month in months:
                etl_web_to_gcs(year, month, color)
        

        if __name__ == "__main__":
        color="yellow"
        months=[2,3]
        year=2019
        etl_parent_flow(color, months, year)

2. Build the deployment:

        prefect deployment build etl_gcs_to_bq.py:etl_parent_flow --name zoomcamp-week2-question3

3. Run the deployment:

        prefect deployment run etl_gcs_to_bq/zoomcamp-week2-question3   
        

## Question 4. Github Storage Block 
Using the CLI:

1. create the block to connect to github from GUI

2. If you run your flow from github it will crash trying to download the dataset to non-existing folder. You have to update the function  ``export_data_local`` and create the folders which you need:  ``data/green ``

        newpath = 'data/green/' 
        if not os.path.exists(newpath):
                os.makedirs(newpath)

3. Build deployment using the block with the param ``-sb`` (storage block) ``github/nameofyourblock``

        prefect deployment build ./week_2_workflow_orchestration/2_homework/04_question/etl_web_to_gcs.py:etl_web_to_gcs --name github_deployment -sb github/zoomgithub -a
        
3. Run deployment

        prefect deployment run etl-web-to-gcs/github_deployment   


Other way to get it is creating the deployment by code instead using the CLI:


1. Create the script github_deployment.py

        from prefect.deployments import Deployment
        from etl_web_to_gcs import etl_web_to_gcs
        from prefect.filesystems import GitHub 

        storage = GitHub.load("zoomgithub")

        deployment = Deployment.build_from_flow(
                flow=etl_web_to_gcs,
                name="github-exercise",
                storage=storage,
                entrypoint="week_2_workflow_orchestration/2_homework/04_question/etl_web_to_gcs.py:etl_web_to_gcs")

        if __name__ == "__main__":
        deployment.apply()

2. Run it to create the deployment

        .\week_2_workflow_orchestration\2_homework\04_question\github_deployment.py

3. Run deployment

        prefect deployment run etl-web-to-gcs/github-exercise


## Question 5. Email or Slack notifications
1. Create an account on app.prefect.cloud

2. Create a workspace on Prefect Server

3. Replicate on Prefect Server the blocks which we've used before: Github and GCS Credentials. On Prefect Cloud doesn't exist ``GCS Bucket``, we have to create it manually with ``gcp_bucket_block.py``:

        from prefect_gcp import GcpCredentials
        from prefect_gcp.cloud_storage import GcsBucket


        bucket_block = GcsBucket(
        gcp_credentials=GcpCredentials.load("zoom-gcp-creds"),
        bucket="dtc_data_lake_digital-aloe-375022",
        )

        bucket_block.save("zoom-gcs", overwrite=True)

Run the script:

        python gcp_bucket_block.py

4. Create an API Key from ``profile settings``

5. From your computer, run the following command (where XXXX is your API Key): 

        prefect cloud login -k XXXXXXXXXX

6. Run ``github_deployment.py`` to create the deployment on Prefect Server

        python github_deployment.py

7. Run deployment:

        prefect deployment run etl-web-to-gcs/github-exercise