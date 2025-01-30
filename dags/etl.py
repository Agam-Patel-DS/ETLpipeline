from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import json


with DAG(
    dag_id = "nasa_apod_postgres",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    # 1. Create the table if it does not exists
    @task
    def create_table():
        # Initialise Postgres Hook
        postgres_hook=PostgresHook(postgres_conn_id="connection")
        # SQL query
        create_table_query="""
        CREATE TABLE IF NOT EXISTS apod_data (
        id SERIAL PRIMARY KEY,
        title VARCHAR(255),
        explanation TEXT,
        url TEXT,
        date DATE,
        media_type VARCHAR(50)
        );
        """
        # Execute the query
        postgres_hook.run(create_table_query)

    # 2. Extract the NASA API data - APOD
    # https://api.nasa.gov/planetary/apod?api_key=gPx6UnajlMX5a4fu0w7YIdP1MnIzthYiRppGXB0L
    extract_apod=SimpleHttpOperator(
        task_id="extract_apod",
        http_conn_id="nasa_api", # Conn Id defined in airflow
        endpoint="planetary/apod", # nasa api endpoints
        method='GET',
        data={"api_key":"{{conn.nasa_api.extra_dejson.api_key}}"}, # Using apir key from the connection
        response_filter= lambda response:response.json() # Convert response to json
    )

    # 3. Transform the data - Information to Save
    @task
    def transform_apod_data(response):
        apod_data = {
            'title': response.get('title',''),
            'explanation': response.get('explanation',''),
            'url': response.get('url',''),
            'date': response.get('date',''),
            'media_type': response.get('media_type','')}
        return apod_data
    
    # 4. Load the data into the Postgres
    @task
    def load_data_to_postgres(apod_data):
        # Initialise the postgres hook
        postgres_hook=PostgresHook(postgres_conn_id='connection')
        # SQL Insert Query
        insert_query="""
        INSERT INTO apod_data (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s);
        """
        # Execute 
        postgres_hook.run(insert_query,parameters=(
            apod_data['title'],
            apod_data['explanation'],
            apod_data['url'],
            apod_data['date'],
            apod_data['media_type']
        ))

    # 5. Verify the data  - DB Viewer
    
    # 6. Define the task dependencies
    # Extract
    create_table() >> extract_apod ## Ensure the table is created before extraction
    api_response=extract_apod.output
    # Transform
    transformed_data=transform_apod_data(api_response)
    # Load
    load_data_to_postgres(transformed_data)