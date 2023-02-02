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

        prefect deployment run etl-web-to-gcs/github_deployment   

