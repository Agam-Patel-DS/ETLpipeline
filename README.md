# ETLpipeline

Workflow (Tasks and their flow in Arifflow):
Steps in the ETL Pipeline:

1. Extract - From the NASA API
2. Transform - According to the Postgres to store
3. Load - Load into the Postgres DB

![Workflow_image](airflow.PNG)

Connection (Connecting the DAG in Airflow to NASA Api and Connection to Postgres running in Docker container):
1. Connection of Postgres running in DOCKER to Airflow
![Connection](Connection.PNG)

2. Connection of NASA Api to Airflow DAG
![Api](api_key.PNG)

![Api](api.PNG)

