FROM jupyter/pyspark-notebook:spark-3.5.0

USER root

# JDBC Postgres
RUN curl -L -o /usr/local/spark/jars/postgresql-42.6.0.jar \
    https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

# Spark-Kafka connector + kafka clients (versões compatíveis com Spark 3.5.0)
RUN wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar -P /usr/local/spark/jars/ && \
    wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar -P /usr/local/spark/jars/

# Python libs (compatíveis)
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copiar app
COPY consumer_spark.py /app/consumer_spark.py
WORKDIR /app

CMD ["spark-submit", "/app/consumer_spark.py"]
