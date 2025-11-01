# 🌦️ Full-Stack On-Premise Data Pipeline for IoT & Weather Analytics

## 📌 Project Overview

This project simulates a real-time weather analytics platform using open-source tools. It ingests weather data from public APIs and synthetic sources, processes it using Apache Spark, stores it in Hive and MySQL, and orchestrates everything with Apache Airflow. Monitoring is enabled via Grafana and Prometheus. The system is designed for on-premise deployment and optionally containerized using Docker Compose.

---

## 🧱 Folder Structure
```
weather_pipeline_project/ ├── airflow/ │ ├── airflow.cfg │ ├── airflow.db │ ├── dags/ │ │ ├── weather_to_kafka_dag.py │ │ ├── faker_ingestion_dag.py │ │ ├── batch_etl_dag.py │ │ └── connectivity_check_dag.py ├── kafka/ │ └── weather_to_kafka.py ├── faker/ │ └── faker_ingestion.py ├── spark/ │ ├── spark_streaming.py │ ├── spark_batch_etl.py │ └── last_etl_timestamp.txt ├── hive/ │ ├── create_final_table.sql │ └── apache-hive-3.1.3-bin/ ├── hadoop/ │ └── hadoop-3.3.6/ ├── csv_parquet_storage/ │ ├── csv_output/ │ ├── parquet_output/ │ ├── hive_final_table_export/ │ └── checkpoint/ ├── spark-events/ │ └── [Spark application logs] ├── kafka-logs/ │ └── [Kafka broker logs] ├── grafana/ │ └── [Grafana setup files] ├── zookeeper-data/ │ └── [Zookeeper metadata]
```
