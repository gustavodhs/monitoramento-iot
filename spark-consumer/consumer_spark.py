import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("TOPIC", "iot.sensors")
POSTGRES_URL = os.getenv("POSTGRES_URL")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "/checkpoints/spark_iot")

schema = StructType([
    StructField("ID_DISPOSITIVO", StringType()),
    StructField("TIMESTAMP", StringType()),
    StructField("TEMPERATURA", DoubleType()),
    StructField("UMIDADE", DoubleType()),
    StructField("LATITUDE", DoubleType()),
    StructField("LONGITUDE", DoubleType()),
    StructField("STATUS", StringType()),
    StructField("BATERIA", DoubleType())
])

spark = SparkSession.builder.appName("iot-spark-consumer").getOrCreate()

# 1. Leitura do Kafka

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

df = df_raw.selectExpr("CAST(value AS STRING) as json_str")
df = df.select(from_json(col("json_str"), schema).alias("d")).select("d.*")

# 2. Transformações

df = df.withColumn("TIMESTAMP_TS", to_timestamp(col("TIMESTAMP")))
df = df.withColumn("ALERTA", (col("TEMPERATURA") > 35) | (col("BATERIA") < 15))

# 3. Grava no PostgreSQL

def write_to_postgres(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    final_df = batch_df.select(
        col("ID_DISPOSITIVO"),
        col("TIMESTAMP_TS"),
        col("TEMPERATURA"),
        col("UMIDADE"),
        col("LATITUDE"),
        col("LONGITUDE"),
        col("STATUS"),
        col("BATERIA"),
        col("ALERTA")
    )

    final_df.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", "public.tb_sensor_evento") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# 4. Streaming Query

query = df.writeStream \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", CHECKPOINT_LOCATION) \
    .start()

query.awaitTermination()
