from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'gowtham',
    'start_date': datetime(2025, 10, 27)
}

dag = DAG(
    'batch_etl_dag',
    default_args=default_args,
    schedule_interval='*/10 * * * *',
    catchup=False,
    max_active_runs=1
)

env_vars = {
    'HIVE_HOME': '/root/weather_pipeline_project/hive/apache-hive-3.1.3-bin',
    'HADOOP_HOME': '/root/weather_pipeline_project/hadoop/hadoop-3.3.6',
    'JAVA_HOME': '/usr/lib/jvm/java-8-openjdk-amd64',
    'PATH': '/root/weather_pipeline_project/hive/apache-hive-3.1.3-bin/bin:/root/weather_pipeline_project/hadoop/hadoop-3.3.6/bin:/usr/lib/jvm/java-8-openjdk-amd64/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin'
}

run_etl = BashOperator(
    task_id='run_batch_etl',
    bash_command='/root/weather_pipeline_project/spark/spark-3.5.7-bin-hadoop3/bin/spark-submit \
        --conf spark.sql.warehouse.dir=/root/spark-warehouse \
        --conf spark.hadoop.hive.metastore.warehouse.dir=/root/spark-warehouse \
        --conf spark.sql.catalogImplementation=hive \
        /root/weather_pipeline_project/spark/spark_batch_etl.py',
    env=env_vars,
    dag=dag
)

root@ip-172-31-24-80:~/weather_pipeline_project# cat airflow/dags/weather_to_kafka_dag.py
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

