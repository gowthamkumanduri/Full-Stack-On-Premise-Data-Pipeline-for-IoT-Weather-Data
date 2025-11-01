from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType

# Initialize Spark session
spark = SparkSession.builder.appName("KafkaToParquet").getOrCreate()

# Define schema for weather JSON
schema = StructType([
    StructField("coord", StructType([
        StructField("lon", DoubleType()),
        StructField("lat", DoubleType())
    ])),
    StructField("main", StructType([
        StructField("temp", DoubleType()),
        StructField("humidity", IntegerType())
    ])),
    StructField("weather", ArrayType(
        StructType([
            StructField("id", IntegerType()),
            StructField("main", StringType()),
            StructField("description", StringType()),
            StructField("icon", StringType())
        ])
    )),
    StructField("name", StringType()),
    StructField("ingest_time", StringType())
])

# Read from Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather-topic") \
    .load()

# Parse Kafka value as JSON
parsed = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

"""
# Print parsed output to console for debugging
query = parsed.select("name", "main.temp", "main.humidity", "ingest_time").writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .trigger(processingTime="120 seconds") \
    .start()
"""

# Write to Parquet every 120 seconds
query = parsed.writeStream \
    .format("parquet") \
    .option("path", "file:///root/weather_pipeline_project/csv_parquet_storage/parquet_output") \
    .option("checkpointLocation", "file:///root/weather_pipeline_project/csv_parquet_storage/checkpoint") \
    .trigger(processingTime="300 seconds") \
    .start()


#Keep the streaming job alive
query.awaitTermination()

