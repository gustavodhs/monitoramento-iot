
# Sistema de Monitoramento IoT

## Introdução
Sistema de streaming que simula sensores IoT enviando dados para Kafka. Um consumidor Spark Structured Streaming processa messages e persiste em PostgreSQL.

## Arquitetura de Solução
- Producer (Python) -> Kafka topic `iot.sensors` -> Spark Structured Streaming -> PostgreSQL
- Componentes orquestrados com Docker Compose.

## Arquitetura Técnica
- Kafka (Confluent images) como mensageria.
- Producer: Python (kafka-python + faker).
- Consumer: Spark Structured Streaming (pyspark) usando `readStream` do Kafka e `foreachBatch` para gravação em PostgreSQL via JDBC.
- Storage: PostgreSQL.
- Checkpoints do Spark em volume Docker para tolerância a falhas.

## Como rodar (pré-requisitos)
- Docker e Docker Compose instalados
- Porta 9092 (Kafka) e 5432 (Postgres) disponíveis

### Passos
1. Clone o repositório:
```bash
git clone <repo-url>
cd iot-streaming-assignment/infra
