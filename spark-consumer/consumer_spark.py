# spark-consumer/consumer_spark.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("TOPIC", "iot.sensors")
POSTGRES_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/iot")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "/checkpoints/spark_iot")

schema = StructType([
    StructField("device_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("status", StringType()),
    StructField("battery", DoubleType())
])

spark = SparkSession.builder \
    .appName("iot-spark-consumer") \
    .getOrCreate()

# Read stream from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

df = df_raw.selectExpr("CAST(value AS STRING) as json_str")
df = df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")
df = df.withColumn("timestamp_ts", to_timestamp(col("timestamp")))

# Transformations
df = df.filter(col("temperature").isNotNull())
df = df.withColumn("alert", (col("temperature") > 35) | (col("battery") < 15))

def write_to_postgres(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    batch_df.select(
        col("device_id").alias("id_dispositivo"),
        col("timestamp_ts").alias("timestamp_ts"),
        col("temperature").alias("temperatura"),
        col("humidity").alias("humidade"),
        "latitude", "longitude", "status",
        col("battery").alias("battery"),
        col("alert").alias("alerta")
    ).write \
      .format("jdbc") \
      .option("url", POSTGRES_URL) \
      .option("dbtable", "public.tb_sensor_evento") \
      .option("user", POSTGRES_USER) \
      .option("password", POSTGRES_PASSWORD) \
      .option("driver", "org.postgresql.Driver") \
      .mode("append") \
      .save()

query = df.writeStream \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", CHECKPOINT_LOCATION) \
    .outputMode("append") \
    .start()

query.awaitTermination()
