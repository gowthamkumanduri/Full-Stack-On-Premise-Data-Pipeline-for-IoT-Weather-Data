from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'gowtham',
    'start_date': datetime(2025, 10, 27)
}

dag = DAG(
    'faker_ingestion_dag',
    default_args=default_args,
    schedule_interval='* * * * *',
    catchup=False,
    max_active_runs=1,
    description='Generates synchronized fake weather logs and sensor data every minute'
)

run_faker_ingestion = BashOperator(
    task_id='generate_csv_and_insert_mysql',
    bash_command='python3 /root/weather_pipeline_project/faker/faker_ingestion.py',
    dag=dag
)
