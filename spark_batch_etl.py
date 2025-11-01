from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lit
from datetime import datetime
import mysql.connector
import logging
import sys
import os

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark session with Hive support
spark = SparkSession.builder \
    .appName("BatchETL") \
    .enableHiveSupport() \
    .getOrCreate()

# Read last ETL timestamp
timestamp_path = "/root/weather_pipeline_project/spark/last_etl_timestamp.txt"
try:
    with open(timestamp_path, "r") as f:
        last_etl_time = f.read().strip()
except FileNotFoundError:
    last_etl_time = "2000-01-01T00:00:00"

last_etl_dt = datetime.fromisoformat(last_etl_time)
logger.info(f"Last ETL timestamp: {last_etl_time}")

# Ensure MySQL final_table exists
try:
    conn = mysql.connector.connect(user='root', password='Inn0vGraph@321', host='localhost')
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS weather_db")
    cursor.execute("USE weather_db")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS final_table (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255),
        city VARCHAR(100),
        temperature FLOAT,
        device_id VARCHAR(255),
        status VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    conn.commit()
    cursor.close()
    conn.close()
except Exception as e:
    logger.error(f"MySQL setup failed: {e}")

# Identify new CSV files based on filename timestamp
csv_dir = "/root/weather_pipeline_project/csv_parquet_storage/csv_output"
new_files = []

for fname in os.listdir(csv_dir):
    if fname.startswith("fake_weather_") and fname.endswith(".csv"):
        ts_str = fname.replace("fake_weather_", "").replace(".csv", "")
        try:
            file_dt = datetime.strptime(ts_str, "%Y%m%d_%H%M%S")
            if file_dt >= last_etl_dt:
                new_files.append(f"file://{os.path.join(csv_dir, fname)}")
        except ValueError:
            continue

# Read and filter CSV
try:
    if new_files:
        csv_df = spark.read.option("header", "true").csv(new_files)
        csv_df = csv_df.withColumn("created_at", to_timestamp(col("created_at")))
        filtered_csv = csv_df.filter(col("created_at") >= to_timestamp(lit(last_etl_time))).alias("csv")
        logger.info(f"Filtered CSV rows: {filtered_csv.count()}")
    else:
        logger.info("No new CSV files found.")
        filtered_csv = None
except Exception as e:
    logger.error(f"CSV read failed: {e}")
    filtered_csv = None

# Read and filter MySQL
try:
    mysql_df = spark.read.format("jdbc").options(
        url="jdbc:mysql://localhost/weather_db",
        driver="com.mysql.cj.jdbc.Driver",
        dbtable="sensors",
        user="root",
        password="Inn0vGraph@321").load()
    mysql_df = mysql_df.withColumn("created_at", to_timestamp(col("created_at")))
    filtered_mysql = mysql_df.filter(col("created_at") >= to_timestamp(lit(last_etl_time))).alias("mysql")
    logger.info(f"Filtered MySQL rows: {filtered_mysql.count()}")
except Exception as e:
    logger.error(f"MySQL read failed: {e}")
    filtered_mysql = None

# Join and transform
try:
    if filtered_csv and filtered_mysql:
        joined_df = filtered_csv.join(filtered_mysql, "device_id")
        transformed_df = joined_df.select(
            col("name"),
            col("city"),
            col("temperature").cast("float"),
            col("device_id"),
            col("status"),
            col("csv.created_at").alias("created_at")
        )
        row_count = transformed_df.count()
        logger.info(f"Transformed row count: {row_count}")
    else:
        row_count = 0
except Exception as e:
    logger.error(f"Join or transform failed: {e}")
    row_count = 0

# Write to Hive, MySQL, and export to CSV
if row_count > 0:
    try:
        logger.info("Writing to Hive table...")
        transformed_df.write.mode("append").insertInto("final_table")
        logger.info("Hive write complete.")

        logger.info("Writing to MySQL table...")
        transformed_df.write.format("jdbc").options(
            url="jdbc:mysql://localhost/weather_db",
            driver="com.mysql.cj.jdbc.Driver",
            dbtable="final_table",
            user="root",
            password="Inn0vGraph@321").mode("append").save()
        logger.info("MySQL write complete.")
    except Exception as e:
        logger.error(f"Write failed: {e}")

    try:
        logger.info("Exporting Hive table to CSV...")
        hive_df = spark.sql("SELECT * FROM final_table")
        hive_df.write.mode("overwrite").csv("file:///root/weather_pipeline_project/csv_parquet_storage/hive_final_table_export", header=True)
        logger.info("CSV export complete.")
    except Exception as e:
        logger.error(f"CSV export failed: {e}")
else:
    logger.info("No new data to process. Skipping write.")

# Update ETL timestamp
try:
    with open(timestamp_path, "w") as f:
        f.write(datetime.now().isoformat())
    logger.info("ETL timestamp updated.")
except Exception as e:
    logger.error(f"Timestamp update failed: {e}")

sys.exit(0)