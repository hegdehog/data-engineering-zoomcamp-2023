1. create the block to connect to github from GUI 

2. Build deployment using the block with the param ``-sb`` (storage block) ``github/nameofyourblock``

        prefect deployment build ./week_2_workflow_orchestration/2_homework/04_question/etl_web_to_gcs.py:etl_web_to_gcs --name github_deployment -sb github/zoomgithub -a
        
3. Run deployment

        deployment run etl-web-to-gcs/github_deployment   