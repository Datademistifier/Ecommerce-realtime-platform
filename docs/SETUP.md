# Setup Guide — Real-Time E-Commerce Analytics Platform

## Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Docker Desktop | 4.x+ | Allocate 6GB RAM in Docker settings |
| Python | 3.9+ | For running producers locally |
| AWS CLI | 2.x | Configured with `aws configure` |
| dbt-core | 1.7+ | `pip install dbt-postgres` |

---

## Step-by-Step Setup

### 1. Environment Configuration
```bash
cp .env.example .env
# Edit .env — fill in your AWS credentials and preferred passwords
```

### 2. Start all Docker services
```bash
docker-compose up -d

# Verify all services are running
docker-compose ps

# Expected: zookeeper, kafka, kafka-ui, spark-master, spark-worker,
#           postgres, airflow-webserver, airflow-scheduler all healthy
```

### 3. Initialize S3 bucket
```bash
pip install boto3
python aws/setup_s3.py
```

### 4. Install Python dependencies for producers
```bash
pip install kafka-python
```

### 5. Start all three event producers
```bash
# In three separate terminals:
python kafka/producers/orders_producer.py
python kafka/producers/clickstream_producer.py
python kafka/producers/inventory_producer.py
```

### 6. Verify events are flowing
```
Open Kafka UI: http://localhost:8090
→ Topics → orders → Messages  (should see live JSON events)
```

### 7. Submit PySpark streaming jobs
```bash
# Orders stream
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.postgresql:postgresql:42.6.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /opt/spark-jobs/stream_orders.py

# Clickstream stream (in a new terminal)
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.postgresql:postgresql:42.6.0 \
  /opt/spark-jobs/stream_clickstream.py
```

### 8. Verify data in PostgreSQL
```bash
docker exec -it postgres psql -U admin -d ecommerce -c \
  "SELECT COUNT(*) FROM raw_events.orders;"
```

### 9. Run dbt transformations
```bash
cd dbt
pip install dbt-postgres
dbt deps
dbt run
dbt test
```

### 10. Access Airflow and enable the DAG
```
Open: http://localhost:8080
Login: admin / admin
Enable DAG: ecommerce_pipeline
Trigger manually for first run
```

---

## Service URLs

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| Spark UI | http://localhost:8081 | — |
| Kafka UI | http://localhost:8090 | — |
| PostgreSQL | localhost:5432 | admin / admin |

---

## Troubleshooting

**Kafka not reachable from producers:**
```bash
# Check kafka is healthy
docker-compose ps kafka
# Should show "healthy"
```

**PySpark can't connect to Kafka:**
```bash
# Use internal Docker hostname inside containers
KAFKA_BOOTSTRAP_SERVERS=kafka:29092  # inside Docker
KAFKA_BOOTSTRAP_SERVERS=localhost:9092  # from host machine
```

**dbt can't connect to Postgres:**
```bash
# Make sure .env has correct POSTGRES_HOST=localhost (running dbt from host)
# or POSTGRES_HOST=postgres (running dbt inside Docker)
```

**Airflow DAG not showing up:**
```bash
docker exec airflow-scheduler airflow dags list
docker exec airflow-scheduler airflow dags trigger ecommerce_pipeline
```
