from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
from dotenv import load_dotenv

# Importing the task functions
from tasks.extract_data import extract_data
from tasks.transform import transform
from tasks.load import load_to_csv 

# Load environment variables
load_dotenv()

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    'etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for extracting, transforming, and loading data',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Define data paths
    DATA_PATH = os.path.join(os.getenv('AIRFLOW_HOME'), 'data')
    DEMO_PATH = os.path.join(DATA_PATH, 'Demographic_Data.csv')
    GEO_PATH = os.path.join(DATA_PATH, 'Geographic_Data.csv')
    OUTPUT_DATA_PATH = os.path.join(DATA_PATH, 'output')

    def extract_demo_task(ti):
        """Task to extract demographic data."""
        demographic_data = extract_data(DEMO_PATH, 'csv')
        ti.xcom_push(key='demographic_data', value=demographic_data)

    def extract_geo_task(ti):
        """Task to extract geographic data."""
        geographic_data = extract_data(GEO_PATH, 'csv')
        ti.xcom_push(key='geographic_data', value=geographic_data)

    def transform_task(ti):
        """Task to transform the extracted data."""
        demographic_data = ti.xcom_pull(key='demographic_data')
        geographic_data = ti.xcom_pull(key='geographic_data')
        transformed_data = transform(demographic_data, geographic_data)
        ti.xcom_push(key='transformed_data', value=transformed_data)

    def load_task(ti):
        """Task to load transformed data to a CSV file with a dynamic filename."""
        transformed_data = ti.xcom_pull(key='transformed_data')
        load_to_csv(transformed_data, OUTPUT_DATA_PATH)

    # Define tasks using PythonOperator
    extract_demo = PythonOperator(
        task_id='extract_demo_task',
        python_callable=extract_demo_task,
    )

    extract_geo = PythonOperator(
        task_id='extract_geo_task',
        python_callable=extract_geo_task,
    )

    transform_operator = PythonOperator(
        task_id='transform_task',
        python_callable=transform_task,
    )

    load_operator = PythonOperator(
        task_id='load_task',
        python_callable=load_task,
    )

    # Set task dependencies
    [extract_demo, extract_geo] >> transform_operator >> load_operator
