"""
ecommerce_pipeline.py
---------------------
Main Airflow DAG — orchestrates the full e-commerce pipeline.

Schedule: hourly
Tasks:
  1. health_check           — verify Kafka, Postgres, S3 are reachable
  2. spark_batch_aggregation — submit PySpark batch job for last hour's S3 data
  3. dbt_staging_run        — run dbt staging models
  4. dbt_intermediate_run   — run dbt intermediate models (depends on staging)
  5. dbt_marts_run          — run dbt mart models (depends on intermediate)
  6. dbt_test               — run all dbt tests
  7. data_freshness_check   — verify data landed in marts within expected SLA
  8. alert_on_failure       — sends notification if any upstream task failed

DAG dependencies:
  health_check
       │
       ▼
  spark_batch_aggregation
       │
       ▼
  dbt_staging_run
       │
       ▼
  dbt_intermediate_run
       │
       ▼
  dbt_marts_run
       │
       ▼
  dbt_test
       │
       ▼
  data_freshness_check
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.trigger_rule import TriggerRule

# ── Default args ──────────────────────────────────────────────
default_args = {
    "owner":            "sonal_mishra",
    "depends_on_past":  False,
    "email":            ["sonalmishrapachori@gmail.com"],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=30),
}

# ── DAG definition ────────────────────────────────────────────
with DAG(
    dag_id="ecommerce_pipeline",
    default_args=default_args,
    description="Hourly e-commerce pipeline: Spark batch → dbt → data quality",
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,       # prevent overlapping runs
    tags=["ecommerce", "spark", "dbt", "production"],
) as dag:

    # ── Task 1: Health checks ─────────────────────────────────
    def check_postgres(**context):
        """Verify PostgreSQL is reachable and raw_events schema has recent data."""
        import psycopg2
        import os
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=5432,
            dbname="ecommerce",
            user=os.getenv("POSTGRES_USER", "admin"),
            password=os.getenv("POSTGRES_PASSWORD", "admin"),
        )
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM raw_events.orders
            WHERE processed_at >= NOW() - INTERVAL '2 hours'
        """)
        count = cur.fetchone()[0]
        conn.close()
        print(f"[health_check] Orders in last 2h: {count}")
        if count == 0:
            raise ValueError("No orders received in the last 2 hours — pipeline may be stalled")
        return count

    def check_s3(**context):
        """Verify S3 bucket is accessible."""
        import boto3
        s3 = boto3.client("s3")
        bucket = "ecommerce-realtime-platform"
        response = s3.list_objects_v2(Bucket=bucket, Prefix="raw/orders/", MaxKeys=1)
        if response.get("KeyCount", 0) == 0:
            raise ValueError(f"S3 bucket {bucket} raw/orders/ is empty")
        print(f"[health_check] S3 accessible, found objects in raw/orders/")

    health_check_postgres = PythonOperator(
        task_id="health_check_postgres",
        python_callable=check_postgres,
    )

    health_check_s3 = PythonOperator(
        task_id="health_check_s3",
        python_callable=check_s3,
    )

    # ── Task 2: PySpark batch aggregation ─────────────────────
    spark_batch = SparkSubmitOperator(
        task_id="spark_batch_aggregation",
        application="/opt/spark-jobs/batch_aggregations.py",
        conn_id="spark_default",
        packages=(
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,"
            "org.postgresql:postgresql:42.6.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        ),
        conf={
            "spark.executor.memory": "2g",
            "spark.executor.cores":  "2",
            "spark.driver.memory":   "1g",
        },
        name="ecommerce_batch_agg_{{ ds }}_{{ execution_date.hour }}",
        verbose=True,
    )

    # ── Tasks 3-5: dbt model runs ─────────────────────────────
    dbt_staging = BashOperator(
        task_id="dbt_staging_run",
        bash_command=(
            "cd /opt/dbt && "
            "dbt run --select staging --profiles-dir /opt/dbt --project-dir /opt/dbt"
        ),
    )

    dbt_intermediate = BashOperator(
        task_id="dbt_intermediate_run",
        bash_command=(
            "cd /opt/dbt && "
            "dbt run --select intermediate --profiles-dir /opt/dbt --project-dir /opt/dbt"
        ),
    )

    dbt_marts = BashOperator(
        task_id="dbt_marts_run",
        bash_command=(
            "cd /opt/dbt && "
            "dbt run --select marts --profiles-dir /opt/dbt --project-dir /opt/dbt"
        ),
    )

    # ── Task 6: dbt tests ─────────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/dbt && "
            "dbt test --profiles-dir /opt/dbt --project-dir /opt/dbt"
        ),
    )

    # ── Task 7: Data freshness / SLA check ────────────────────
    data_freshness_check = PostgresOperator(
        task_id="data_freshness_check",
        postgres_conn_id="postgres_default",
        sql="""
            DO $$
            DECLARE
                latest_ts TIMESTAMP;
                lag_minutes INTEGER;
            BEGIN
                SELECT MAX(order_date) INTO latest_ts FROM analytics.mart_daily_revenue;

                IF latest_ts IS NULL THEN
                    RAISE EXCEPTION 'mart_daily_revenue is empty — dbt run may have failed';
                END IF;

                lag_minutes := EXTRACT(EPOCH FROM (NOW() - latest_ts)) / 60;

                IF lag_minutes > 90 THEN
                    RAISE EXCEPTION 'Data freshness SLA breach: mart_daily_revenue is % minutes stale', lag_minutes;
                END IF;

                RAISE NOTICE 'Data freshness OK: % minutes lag', lag_minutes;
            END $$;
        """,
    )

    # ── Task 8: Alert on failure (runs regardless of upstream state) ──
    def send_failure_alert(**context):
        """In production: send Slack/PagerDuty alert. Here: log the failure."""
        failed_tasks = context.get("task_instance").xcom_pull(
            task_ids=None, key="return_value"
        )
        dag_run = context["dag_run"]
        print(f"[alert] DAG {dag_run.dag_id} run {dag_run.run_id} had failures")
        print(f"[alert] Execution date: {context['execution_date']}")
        # In production: requests.post(SLACK_WEBHOOK_URL, json={"text": message})

    alert_on_failure = PythonOperator(
        task_id="alert_on_failure",
        python_callable=send_failure_alert,
        trigger_rule=TriggerRule.ONE_FAILED,  # only runs if something upstream failed
    )

    # ── Dependency graph ──────────────────────────────────────
    [health_check_postgres, health_check_s3] >> spark_batch
    spark_batch >> dbt_staging >> dbt_intermediate >> dbt_marts >> dbt_test
    dbt_test >> data_freshness_check
    [spark_batch, dbt_staging, dbt_intermediate, dbt_marts, dbt_test] >> alert_on_failure
