# Sistema de Monitoramento IoT em Tempo Real

## Introdução
Este projeto implementa um pipeline de streaming completo para monitoramento de sensores IoT em tempo real.

Ele atende o desafio técnico proposte, utilizando:
- Producer gerando dados IoT simulados (temperatura, umidade, coordenadas, status etc.)
- Apache Kafka como mensageria
- Apache Spark Structured Streaming como consumer Big Data
- PostgreSQL como destino final dos dados processados
- Docker e Docker Compose para reprodutibilidade

A solução foi projetada seguindo princípios de escalabilidade, resiliência, tolerância a falhas e arquitetura moderna para Big Data.

## Arquitetura de Solução
- Producer (Python) -> Kafka topic `iot.sensors` -> Spark Structured Streaming -> PostgreSQL

+----------------+       +---------------------+       +-----------------------+
|   Producer     | --->  |       Kafka         | --->  |   Spark Structured    |
| (Python+Faker) |       |  (Mensageria real)  |       |      Streaming        |
+----------------+       +---------------------+       +-----------------------+
                                                                  |
                                                                  v
                                                        +---------------------+
                                                        |     PostgreSQL      |
                                                        | (Armazenamento DB) |
                                                        +---------------------+


- Componentes orquestrados com Docker Compose.

## Tecnologias utilizadas
- Python 3.10
- Faker para simulação de dados IoT
- Kafka + Zookeeper (Confluent Platform)
- Spark 3.5.0
- Structured Streaming com checkpointing
- PostgreSQL 15
- Docker & Docker Compose
- JDBC Driver + Kafka connectors

## Arquitetura Técnica
- Kafka (Confluent images) como mensageria.
- Producer: Python (kafka-python + faker).
- Consumer: Spark Structured Streaming (pyspark) usando `readStream` do Kafka e `foreachBatch` para gravação em PostgreSQL via JDBC.
- Storage: PostgreSQL.
- Checkpoints do Spark em volume Docker para tolerância a falhas.

## Como Executar o Projeto (pré-requisitos)
- Docker e Docker Compose instalados
- Porta 9092 (Kafka) e 5432 (Postgres) disponíveis
- Git

### Passos
1. Clone o repositório:
```bash
git clone https://github.com/gustavodhs/monitoramento-iot
cd monitoramento-iot/infra
```

2. Iniciar o Docker:
Abra o menu iniciar → digite: Docker Desktop
Clique na aplicação e espere até ele mostrar: Docker is Running

3. Subir todas as dependências:
```bash
docker compose up --build
```

3. Verificar dados no PostgreSQL
```bash
docker exec -it infra-postgres-1 psql -U postgres -d iot -c "SELECT * FROM sensor_events;"
docker exec -it infra-postgres-1 psql -U postgres -d iot -c "SELECT COUNT(*) FROM sensor_events;"
```

## Explicação do Case (Plano de Implementação)
### 1. Objetivo
Criar um sistema completo de monitoramento IoT em real-time para ingestão, processamento e armazenamento seguro de dados.

### 2. Fluxo do sistema IoT
1. Gerar eventos IoT artificialmente (temperatura, umidade, GPS etc.)
2. Enviar eventos para Kafka (Producer)
3. Consumir eventos com Spark (estrutura distribuída)
4. Transformar e enriquecer os dados
5. Persistir no PostgreSQL

### 3. Lógica de Negócio
- Todos os eventos são validados e convertidos via schema Spark.
- Timestamps são normalizados via to_timestamp.
- Cálculo de alerta:
```python
df = df.withColumn("alert", (col("temperature") > 35) | (col("battery") < 15))
```

- Todos os eventos são persistidos com:
+ ID
+ ID_DISPOSITIVO
+ TIMESTAMP original
+ TIMESTAMP_TS (convertido)
+ TEMPERATURA
+ HUMIDADE
+ LATITUDE
+ LONGITUDE
+ STATUS
+ BATERIA
+ ALERTA
+ DT_CARGA

## Melhorias Futuras
### Técnicas

- Criar API REST para consultar eventos recentes
- Dashboards (Grafana / Power BI)
- Deploy em Kubernetes (K8s)
- Adicionar testes unitários
- Inserir Data Lake (S3 ou MinIO)
- Criar camada de enriquecimento com ML (detecção de anomalias)

### De arquitetura

- Criar (raw → bronze → silver)
- Adicionar schema registry (confluent)

### Cloud

- Migrar pipeline para:

AWS Glue + EMR
Azure Data Factory
Databricks
