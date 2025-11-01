import csv
import os
import random
import logging
from datetime import datetime
from faker import Faker
import mysql.connector

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Shared timestamp
timestamp = datetime.now().isoformat()
filename_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

# Constants
cities = ["Delhi", "Mumbai", "Chennai", "Kolkata", "Bangalore"]
city_device_map = {
    "Delhi": "dev-delhi-001",
    "Mumbai": "dev-mumbai-002",
    "Chennai": "dev-chennai-003",
    "Kolkata": "dev-kolkata-004",
    "Bangalore": "dev-bangalore-005"
}
csv_dir = "/root/weather_pipeline_project/csv_parquet_storage/csv_output"
csv_path = os.path.join(csv_dir, f"fake_weather_{filename_ts}.csv")

def generate_csv():
    fake = Faker()
    os.makedirs(csv_dir, exist_ok=True)
    with open(csv_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["name", "city", "temperature", "device_id", "created_at"])
        for _ in range(10):
            city = random.choice(cities)
            writer.writerow([
                fake.name(),
                city,
                round(random.uniform(20, 40), 2),
                city_device_map[city],
                timestamp
            ])
    logger.info(f"CSV generated: {csv_path}")

def insert_mysql():
    try:
        conn = mysql.connector.connect(user='root', password='Inn0vGraph@321', host='localhost')
        cursor = conn.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS weather_db")
        cursor.execute("USE weather_db")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS sensors (
            id INT AUTO_INCREMENT PRIMARY KEY,
            device_id VARCHAR(255) UNIQUE,
            location VARCHAR(100),
            status VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        rows = [(city_device_map[city], city, random.choice(['active', 'inactive'])) for city in cities]
        cursor.executemany("INSERT IGNORE INTO sensors (device_id, location, status) VALUES (%s, %s, %s)", rows)
        conn.commit()
        logger.info("Inserted sensor records into MySQL.")
    except mysql.connector.Error as err:
        logger.error(f"MySQL error: {err}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    generate_csv()
    insert_mysql()

