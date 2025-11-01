# 🌦️ Full-Stack On-Premise Data Pipeline for IoT & Weather Analytics

## 📌 Project Overview

This project simulates a real-time weather analytics platform using open-source tools. It ingests weather data from public APIs and synthetic sources, processes it using Apache Spark, stores it in Hive and MySQL, and orchestrates everything with Apache Airflow. Monitoring is enabled via Grafana and Prometheus. The system is designed for on-premise deployment and optionally containerized using Docker Compose.

---

## 🧱 Folder Structure

---

weather_pipeline_project/ ├── airflow/ │ ├── airflow.cfg │ ├── airflow.db │ ├── dags/ │ │ ├── weather_to_kafka_dag.py │ │ ├── faker_ingestion_dag.py │ │ ├── batch_etl_dag.py │ │ └── connectivity_check_dag.py ├── kafka/ │ └── weather_to_kafka.py ├── faker/ │ └── faker_ingestion.py ├── spark/ │ ├── spark_streaming.py │ ├── spark_batch_etl.py │ └── last_etl_timestamp.txt ├── hive/ │ ├── create_final_table.sql │ └── apache-hive-3.1.3-bin/ ├── hadoop/ │ └── hadoop-3.3.6/ ├── csv_parquet_storage/ │ ├── csv_output/ │ ├── parquet_output/ │ ├── hive_final_table_export/ │ └── checkpoint/ ├── spark-events/ │ └── [Spark application logs] ├── kafka-logs/ │ └── [Kafka broker logs] ├── grafana/ │ └── [Grafana setup files] ├── zookeeper-data/ │ └── [Zookeeper metadata]

---
---

## ⚙️ Technologies Used

| Tool               | Purpose                          |
|--------------------|----------------------------------|
| Python + Faker     | Synthetic data generation        |
| Apache Kafka       | Real-time ingestion              |
| Apache Spark       | Streaming + Batch ETL            |
| Apache Hive        | Data lake storage                |
| MySQL              | Relational storage               |
| Apache Airflow     | Workflow orchestration           |
| Grafana + Prometheus | Monitoring (optional)         |
| Docker Compose     | Containerized deployment (optional)

---

## 🚀 Pipeline Components

### 🔹 1. Ingestion

- **Weather API to Kafka**  
  - Script: `kafka/weather_to_kafka.py`  
  - DAG: `weather_to_kafka_dag.py`  
  - Pulls weather data from OpenWeatherMap every minute and pushes to Kafka topic `weather-topic`.

- **Faker to CSV + MySQL**  
  - Script: `faker/faker_ingestion.py`  
  - DAG: `faker_ingestion_dag.py`  
  - Generates synthetic weather logs and inserts mock sensor data into MySQL every minute.

---

### 🔹 2. Processing

- **Spark Streaming**  
  - Script: `spark/spark_streaming.py`  
  - Managed via `systemd` (not Airflow)  
  - Consumes Kafka topic and writes Parquet files every 5 minutes to `csv_parquet_storage/parquet_output`.

- **Spark Batch ETL**  
  - Script: `spark/spark_batch_etl.py`  
  - DAG: `batch_etl_dag.py`  
  - Joins new CSV files + MySQL sensor data, transforms, and loads into Hive and final MySQL table.

---

### 🔹 3. Storage

- **Parquet Files**  
  - Stored in `csv_parquet_storage/parquet_output/` from Spark Streaming.

- **Hive Table**  
  - Table: `final_table`  
  - Created via `hive/create_final_table.sql`

- **MySQL Final Table**  
  - Table: `final_table` in `weather_db`  
  - Populated via Spark Batch ETL

- **CSV Export from Hive**  
  - Output: `csv_parquet_storage/hive_final_table_export/`

---

### 🔹 4. Orchestration

- **Apache Airflow**  
  - DAGs scheduled for ingestion and batch ETL
  - Uses `LocalExecutor` with MySQL backend
  - DAGs are modular and non-overlapping (`max_active_runs=1`)

---

## 🧪 Running the Project

### Prerequisites

- Python 3.8+
- Kafka, Spark, Hive, MySQL installed and configured
- Airflow initialized with MySQL backend
- Optional: Docker Compose setup

### Steps

1. Start Kafka, MySQL, Hive, Spark services
2. Initialize Airflow:
   ```bash
   airflow db init
   airflow users create --role Admin --username gowtham ...

Unpause DAGs:

airflow dags unpause weather_to_kafka
airflow dags unpause faker_ingestion_dag
airflow dags unpause batch_etl_dag

Start Spark Streaming via systemd:

sudo systemctl start spark-streaming.service

Monitor DAGs via Airflow UI (http://<host>:5050)

Validate Kafka topic, Parquet files, Hive tables, and MySQL final table.
