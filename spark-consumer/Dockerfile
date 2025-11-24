FROM bitnami/spark:latest
USER root
WORKDIR /app
COPY requirements.txt .
# instalar pyspark deps (se necessário)
RUN pip install --no-cache-dir -r requirements.txt
COPY consumer_spark.py .
CMD ["bash", "-lc", "spark-submit --deploy-mode client consumer_spark.py"]
