from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'gowtham',
    'start_date': datetime(2025, 10, 27)
}

dag = DAG(
    'weather_to_kafka',
    default_args=default_args,
    schedule_interval='* * * * *',
    catchup=False,
    max_active_runs=1
)

push_weather = BashOperator(
    task_id='push_weather_to_kafka',
    bash_command='python3 /root/weather_pipeline_project/kafka/weather_to_kafka.py',
    dag=dag
)
