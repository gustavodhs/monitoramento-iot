# spark-consumer/consumer_spark.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, schema_of_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("TOPIC", "iot.sensors")
POSTGRES_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/iot")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
CHECKPOINT_LOCATION = "/checkpoints/spark_iot"

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
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# messages are in value as bytes
df = df_raw.selectExpr("CAST(value AS STRING) as json_str")
df = df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

# convert timestamp string to timestamp type
df = df.withColumn("timestamp_ts", to_timestamp(col("timestamp")))

# example transformations: filter invalid temps, compute alert flag
df = df.filter(col("temperature").isNotNull())
df = df.withColumn("alert", (col("temperature") > 35) | (col("battery") < 15))

def write_to_postgres(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    batch_df.select(
        "device_id", "timestamp_ts", "temperature", "humidity", "latitude", "longitude", "status", "battery", "alert"
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
