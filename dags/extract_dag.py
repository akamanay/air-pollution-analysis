from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

# Importing the task functions
from extract.extract_data import extract_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'first_extract_dag',
    default_args=default_args,
    description='First extract DAG',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
)

# Define the tasks
extract_demo = PythonOperator(
    task_id='extract_1',
    python_callable=extract_data('data/Demographic_Data.csv', 'csv'),
    dag=dag,
)

extract_geo = PythonOperator(
    task_id='extract_2',
    python_callable=extract_data('data/Geographic_Data.csv', csv),
    dag=dag,
)

extract_simple_api = PythonOperator(
    task_id='extract_3',
    python_callable=extract_data('https://jsonplaceholder.typicode.com/posts', 'api'),
    dag=dag,
)

# Set the task dependencies
[extract_demo, extract_geo, extract_simple_api] >> transform >> load
