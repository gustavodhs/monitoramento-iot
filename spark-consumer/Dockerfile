FROM jupyter/pyspark-notebook:spark-3.5.0

# Rodar como root para evitar o bug do user.home = "?"
USER root

# Diretório padrão de jars do Spark nessa imagem
ENV SPARK_JARS_DIR=/usr/local/spark/jars

# Baixar os jars necessários para Kafka + Postgres + Commons Pool
RUN set -eux && \
    mkdir -p "$SPARK_JARS_DIR" && \
    curl -L -o "$SPARK_JARS_DIR/spark-sql-kafka-0-10_2.12-3.5.0.jar" \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -L -o "$SPARK_JARS_DIR/spark-token-provider-kafka-0-10_2.12-3.5.0.jar" \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    curl -L -o "$SPARK_JARS_DIR/kafka-clients-3.5.0.jar" \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar && \
    curl -L -o "$SPARK_JARS_DIR/commons-pool2-2.11.1.jar" \
      https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    curl -L -o "$SPARK_JARS_DIR/postgresql-42.7.3.jar" \
      https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

WORKDIR /app

COPY consumer_spark.py /app/consumer_spark.py

CMD ["spark-submit", "/app/consumer_spark.py"]
