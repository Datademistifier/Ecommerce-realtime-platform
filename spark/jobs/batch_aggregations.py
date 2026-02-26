"""
batch_aggregations.py
---------------------
PySpark batch job — runs hourly via Airflow.

Reads from S3 raw Parquet (previous hour's data),
joins orders + clickstream, and writes enriched
aggregates back to PostgreSQL analytics schema.

Triggered by: Airflow DAG task 'spark_batch_aggregation'
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime, timedelta, timezone
import os
import sys

S3_BUCKET      = os.getenv("S3_BUCKET_NAME", "ecommerce-realtime-platform")
POSTGRES_URL   = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST','postgres')}:5432/ecommerce"
POSTGRES_PROPS = {
    "user":     os.getenv("POSTGRES_USER", "admin"),
    "password": os.getenv("POSTGRES_PASSWORD", "admin"),
    "driver":   "org.postgresql.Driver",
}

def create_spark_session():
    return (
        SparkSession.builder
        .appName("EcommerceBatchAggregations")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", ""))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", ""))
        .getOrCreate()
    )


def get_processing_window():
    """Get the hour window to process (default: last complete hour)."""
    now = datetime.now(timezone.utc)
    end   = now.replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(hours=1)
    return start, end


def run(spark: SparkSession, run_date: str = None):
    start, end = get_processing_window()
    date_str = start.strftime("%Y-%m-%d")
    print(f"[batch_agg] Processing window: {start} → {end}")

    # ── Read raw orders from S3 ───────────────────────────────
    orders_path = f"s3a://{S3_BUCKET}/raw/orders/event_date={date_str}/"
    try:
        orders_df = (
            spark.read.parquet(orders_path)
            .filter(
                (F.col("event_timestamp") >= F.lit(start.isoformat()))
                & (F.col("event_timestamp") < F.lit(end.isoformat()))
            )
        )
        order_count = orders_df.count()
        print(f"[batch_agg] Loaded {order_count} orders from S3")
    except Exception as e:
        print(f"[batch_agg] WARNING: Could not read orders from S3: {e}")
        return

    if order_count == 0:
        print("[batch_agg] No orders in window — skipping.")
        return

    # ── Hourly revenue by category ────────────────────────────
    hourly_revenue = (
        orders_df
        .groupBy("category", "shipping_state")
        .agg(
            F.count("order_id").alias("order_count"),
            F.sum("order_total").alias("total_revenue"),
            F.avg("order_total").alias("avg_order_value"),
            F.sum("quantity").alias("total_items"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )
        .withColumn("processing_hour", F.lit(start.isoformat()))
        .withColumn("processed_at", F.current_timestamp())
    )

    hourly_revenue.write.jdbc(
        POSTGRES_URL,
        "raw_events.hourly_revenue_by_category",
        mode="append",
        properties=POSTGRES_PROPS,
    )
    print(f"[batch_agg] Wrote {hourly_revenue.count()} category revenue rows")

    # ── Top products this hour ────────────────────────────────
    top_products = (
        orders_df
        .groupBy("product_id", "product_name", "category")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.sum("order_total").alias("revenue"),
            F.count("order_id").alias("order_count"),
        )
        .withColumn(
            "revenue_rank",
            F.rank().over(
                __import__("pyspark.sql.window", fromlist=["Window"])
                .Window.partitionBy("category")
                .orderBy(F.col("revenue").desc())
            )
        )
        .withColumn("processing_hour", F.lit(start.isoformat()))
        .withColumn("processed_at", F.current_timestamp())
    )

    top_products.write.jdbc(
        POSTGRES_URL,
        "raw_events.hourly_top_products",
        mode="append",
        properties=POSTGRES_PROPS,
    )
    print(f"[batch_agg] Wrote {top_products.count()} product rows")
    print(f"[batch_agg] Batch job complete for window {start} → {end}")


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    run_date = sys.argv[1] if len(sys.argv) > 1 else None
    run(spark, run_date)
    spark.stop()


if __name__ == "__main__":
    main()
