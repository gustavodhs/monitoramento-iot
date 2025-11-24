# spark-consumer/Dockerfile (VERSÃO CORRIGIDA)

FROM jupyter/pyspark-notebook:spark-3.5.0

USER root

# Altere o caminho de instalação do driver JDBC
# O caminho correto para a imagem Jupyter é geralmente /usr/local/spark/jars/
RUN curl -o /usr/local/spark/jars/postgresql-42.6.0.jar https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

# libs Python
RUN pip install --no-cache-dir kafka-python delta-spark

COPY consumer_spark.py /app/consumer_spark.py
WORKDIR /app

# spark-submit já estará disponível no PATH
CMD ["spark-submit", "/app/consumer_spark.py"]