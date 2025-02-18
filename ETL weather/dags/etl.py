from airflow import DAG
from airflow.providers.https.hooks.http import HttpHook
from airflow.providers.postgress.hooks.postgress import PostgresHook
from airflow.decorators import task 
from airflow.utils.dates import days_ago
import requests
import json
from pandas import json_normalize
from datetime import datetime

# latitude and longitude for the desired location
latitude="51.5074"
longitude="-0.1278"
postgres_conn_id="postgres_default"
api_conn_id="open_mero_api"


default_args={
    "owner":"airflow"
    "start_date":days_ago(1)
}

#DAG
with DAG(dag_id='weather_etl_pipeline'start_date=datetime(2024,9,1),
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False) as dag:

    @task()
    def extract_weather_data():
        """extracting weather data from open-Meteo Api using Airflow connetion ."""
        # http hook to get details from Airflow connection
        http_hook=HttpHook(http_conn_id=api_conn_id,method='GET')
        ##Build the API endpoint
        # https://api.open-meteo.com//v1/forecast?latitude=51.5074&longitude=-0.1278&current_weather=true
        endpoint=f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'
        
        # make the request via the Http hook
        response=http_hook.run(endpoint)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"failed to fetch weather data":{response.status_code}")

    @task()
    def transform_weather_data(weather_data):
        """transform the extracted weather data"""
        current_weather=weather_data['current_weather']
        transformed_data={
            'latitude':LATITUDE,
            'longitude':LONGITUDE,
            'TEMPERATURE':current_weather['temperature'],
            'windspeed':current_weather['windspeed'],
            'winddirection':current_weather['winddirection'],
            'weathercode':current_weather['weathercode']
        }
        return transformed_data

    @taks()
    def load_weather_data(transformed_data):
        """load transformed data into weather"""
        pg_hook=PostgresHook(postgres_conn_id=postgres_conn_id)
        conn=pg_hook.get_conn()
        cursor=conn.cursor()
# create tale if not exists
        cursor.execute(""")
        CREATE TABLE IF NOT EXISTS weather_data (
            latitude FLOAT,
            longitude FLOAT,
            temperature FLOAT,
            windspeed FLOAT,
            winddirection FLOAT,
            weathercode INT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP

        );
        """)
    # @task()
    # def load_weather_data(transform_data):
    #     """load transformed data into postgressSQL"""
    #     pg_hook=PostgresHook(postgres_conn_id=postgres_conn_id)
    #     conn=pg_hook.get_conn()
    #     cursor=conn.cursor()


# # create tale if not exists
#         cursor.execute(""")
#         CREATE TABLE IF NOT EXISTS weather_data (
#             latitude FLOAT,
#             longitude FLOAT,
#             temperature FLOAT,
#             windspeed FLOAT,
#             winddirection FLOAT,
#             weathercode INT,
#             timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP

        # );
        # """)
# insert transform data into the table
cursor.execute("""
INSERT INTO weather_data (latitude,longitude,temperature,winddirection,windspeed,weathercode)
VALUES (%s,%s,%s,%s,%s)
""",(
    transformed_data['latitude'],
    transformed_data['longitude'],
    transformed_data['temperature'],
    transformed_data['windspeed'],
    transformed_data['windirection'],
    transformed_data['weathercode']
    ))

    conn.commit()
    cursor.close()

#DAG WORKFLOW=ETL PIPELINE 
weather_data=extract_weather_data()
transform_data=transform_weather_data(weather_data)
load_weather_data(transform_data)