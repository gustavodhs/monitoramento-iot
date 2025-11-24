# producer/producer.py
import json
import time
import random
import logging
from faker import Faker
from kafka import KafkaProducer
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("producer")

fake = Faker()

KAFKA_BROKER = "kafka:9092"
TOPIC = "iot.sensors"

def gen_sensor_event():
    # garante floats para latitude/longitude (sem Decimal)
    lat = float(fake.latitude())
    lon = float(fake.longitude())
    return {
        "device_id": f"device-{random.randint(1,50)}",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "temperature": round(random.uniform(10.0, 40.0), 2),
        "humidity": round(random.uniform(10.0, 95.0), 2),
        "latitude": round(lat, 6),
        "longitude": round(lon, 6),
        "status": random.choice(["OK","WARN","ERROR"]),
        "battery": round(random.uniform(10, 100), 2)
    }

def main():
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v, default=float).encode('utf-8'),
        retries=5,
        linger_ms=10
    )
    logger.info("Producer iniciado, enviando eventos para %s", TOPIC)
    try:
        while True:
            event = gen_sensor_event()
            producer.send(TOPIC, value=event)
            logger.info("Enviado: %s", event)
            time.sleep(random.uniform(0.2, 1.0))
    except KeyboardInterrupt:
        logger.info("Producer finalizando...")
    except Exception as e:
        logger.exception("Erro no producer: %s", e)
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()
