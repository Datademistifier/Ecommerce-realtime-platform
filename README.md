# 🛒 Real-Time E-Commerce Analytics Platform

> **An end-to-end streaming data pipeline that ingests live e-commerce events through Apache Kafka, processes them with PySpark Structured Streaming, stores raw data on AWS S3, transforms it with dbt, and orchestrates everything with Apache Airflow — all running locally via Docker Compose.**

Built by **Sonal Mishra** — Data Engineer | Snowflake Squad Member | SnowPro Associate Certified | Austin User Group Leader

📎 [LinkedIn](https://www.linkedin.com/in/mishrasonal) | 🔗 [Portfolio](https://github.com/Datademistifier)

---

## 📌 What This Project Demonstrates

| Tool / Skill | Role in This Project |
|---|---|
| **Apache Kafka** | Streaming backbone — produces order, clickstream, and inventory events |
| **PySpark Structured Streaming** | Consumes Kafka topics, applies transformations, writes to S3 and Postgres |
| **Apache Airflow** | Orchestrates dbt runs, Spark batch jobs, data quality checks |
| **dbt (data build tool)** | Transforms raw data into staging → intermediate → mart layers with tests |
| **PostgreSQL** | Analytical database — serves the final mart layer |
| **AWS S3** | Data lake — raw event landing zone before transformation |
| **Docker Compose** | One-command local setup — runs all services together |
| **Python** | Kafka producers, PySpark jobs, Airflow operators, utility scripts |
| **SQL** | dbt models, Postgres queries, data quality tests |

---

## 🏗️ Architecture

```
                    ┌─────────────────────────────────────────────────────┐
                    │                  EVENT PRODUCERS                     │
                    │  orders_producer.py  │  clickstream_producer.py      │
                    │  inventory_producer.py                               │
                    └──────────────┬──────────────────────────────────────┘
                                   │  (Python → kafka-python)
                                   ▼
                    ┌──────────────────────────────┐
                    │       APACHE KAFKA           │
                    │  Topic: orders               │
                    │  Topic: clickstream          │
                    │  Topic: inventory_updates    │
                    └──────────────┬───────────────┘
                                   │
                    ┌──────────────▼───────────────┐
                    │    PYSPARK STRUCTURED        │
                    │       STREAMING              │
                    │  - Schema enforcement        │
                    │  - Windowed aggregations     │
                    │  - Data quality filters      │
                    └──────┬───────────────┬───────┘
                           │               │
              ┌────────────▼──┐    ┌───────▼──────────┐
              │   AWS S3      │    │   PostgreSQL      │
              │  (Data Lake)  │    │  (raw_events      │
              │  raw/orders/  │    │   schema)         │
              │  raw/clicks/  │    └───────┬──────────┘
              └───────────────┘            │
                                  ┌────────▼──────────┐
                                  │       dbt         │
                                  │  staging/ models  │
                                  │  intermediate/    │
                                  │  marts/           │
                                  └────────┬──────────┘
                                           │
                                  ┌────────▼──────────┐
                                  │   APACHE AIRFLOW  │
                                  │  DAG: ecommerce_  │
                                  │  pipeline         │
                                  │  (orchestrates    │
                                  │   everything)     │
                                  └───────────────────┘
```

---

## 📁 Repository Structure

```
ecommerce-realtime-platform/
├── README.md
├── docker-compose.yml              ← spins up ALL services
├── .env.example                    ← environment variable template
│
├── kafka/
│   ├── producers/
│   │   ├── orders_producer.py      ← simulates live order events
│   │   ├── clickstream_producer.py ← simulates page view / cart events
│   │   └── inventory_producer.py  ← simulates stock update events
│   └── consumers/
│       └── verify_consumer.py     ← debug: read raw events from topics
│
├── spark/
│   └── jobs/
│       ├── stream_orders.py        ← PySpark: orders Kafka → S3 + Postgres
│       ├── stream_clickstream.py   ← PySpark: clicks Kafka → S3 + Postgres
│       └── batch_aggregations.py  ← PySpark: hourly batch aggregation job
│
├── airflow/
│   └── dags/
│       ├── ecommerce_pipeline.py   ← main orchestration DAG
│       └── dbt_run_dag.py         ← dedicated dbt DAG
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_orders.sql
│   │   │   ├── stg_clickstream.sql
│   │   │   └── stg_inventory.sql
│   │   ├── intermediate/
│   │   │   ├── int_order_items_enriched.sql
│   │   │   └── int_user_sessions.sql
│   │   └── marts/
│   │       ├── mart_daily_revenue.sql
│   │       ├── mart_product_performance.sql
│   │       └── mart_user_behavior.sql
│   └── tests/
│       ├── assert_order_total_positive.sql
│       └── assert_no_duplicate_order_ids.sql
│
├── postgres/
│   └── init/
│       └── 01_create_schemas.sql  ← raw_events + analytics schemas
│
├── aws/
│   └── setup_s3.py                ← creates S3 buckets + folder structure
│
├── data/
│   └── sample/
│       ├── products.csv           ← seed data: product catalog
│       └── users.csv              ← seed data: user master
│
└── docs/
    └── SETUP.md                   ← step-by-step run instructions
```

---

## 🚀 Quick Start

### Prerequisites
- Docker Desktop (4GB+ RAM allocated)
- AWS account (free tier) + configured `~/.aws/credentials`
- Python 3.9+

### Step 1 — Clone and configure
```bash
git clone https://github.com/Datademistifier/ecommerce-realtime-platform
cd ecommerce-realtime-platform
cp .env.example .env
# Edit .env with your AWS credentials and config
```

### Step 2 — Start all services
```bash
docker-compose up -d
# Starts: Kafka, Zookeeper, Spark, Airflow, PostgreSQL
```

### Step 3 — Set up AWS S3 buckets
```bash
python aws/setup_s3.py
```

### Step 4 — Start event producers
```bash
python kafka/producers/orders_producer.py &
python kafka/producers/clickstream_producer.py &
python kafka/producers/inventory_producer.py &
```

### Step 5 — Submit Spark streaming jobs
```bash
docker exec spark-master spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
  /opt/spark-jobs/stream_orders.py
```

### Step 6 — Trigger the Airflow DAG
```
Open: http://localhost:8080
Login: admin / admin
Enable DAG: ecommerce_pipeline
```

### Step 7 — Run dbt transformations
```bash
cd dbt && dbt run && dbt test
```

---

## 🧠 Skills Demonstrated

`Apache Kafka` `PySpark Structured Streaming` `Apache Airflow` `dbt`
`PostgreSQL` `AWS S3` `Docker Compose` `Python` `SQL` `Data Modeling`
`Stream Processing` `Batch Processing` `Data Quality Testing` `Orchestration`
`Data Lake Architecture` `Event-Driven Design` `ETL/ELT` `CDC Patterns`

---

## 📬 Contact

**Sonal Mishra** — sonalmishrapachori@gmail.com | [LinkedIn](https://www.linkedin.com/in/mishrasonal)
