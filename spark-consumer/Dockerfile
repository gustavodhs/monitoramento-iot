FROM jupyter/pyspark-notebook:spark-3.5.0

# ===========================
# FIX 1 – Definir HOME válido
# ===========================
USER root
ENV HOME=/home/spark
RUN mkdir -p $HOME/.ivy2 && chown -R 1000:100 $HOME

USER root

RUN mkdir -p /checkpoints \
    && chown -R 1001:1001 /checkpoints

# ======================================
# FIX 2 – Desativar completamente o Ivy
# ======================================
ENV SPARK_JARS_IVY_DISABLE=true
ENV PYSPARK_SUBMIT_ARGS="--conf spark   .jars.ivyDisable=true pyspark-shell"

# ===============================
# FIX 3 – Instalar os conectores
# no diretório REAL do Spark
# ===============================
RUN mkdir -p /usr/local/spark/jars && \
    curl -L -o /usr/local/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -L -o /usr/local/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    curl -L -o /usr/local/spark/jars/kafka-clients-3.5.0.jar \
        https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar && \
    curl -L -o /usr/local/spark/jars/postgresql-42.7.4.jar \
        https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar
    curl -L -o /opt/bitnami/spark/jars/postgresql-42.7.3.jar \
        https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

# ==========================
# Voltar ao usuário notebook
# ==========================
USER 1000

# Copiar o consumer
COPY consumer_spark.py /app/consumer_spark.py

CMD ["spark-submit", "/app/consumer_spark.py"]
