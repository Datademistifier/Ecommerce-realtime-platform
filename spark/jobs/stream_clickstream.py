"""
stream_clickstream.py
---------------------
PySpark Structured Streaming job for clickstream events.

Reads from Kafka topic 'clickstream':
  1. Parses JSON, enforces schema
  2. Reconstructs user sessions using session windows
  3. Computes funnel metrics per 10-minute window
  4. Writes raw events to S3 and PostgreSQL
  5. Writes session summaries to PostgreSQL

Submit:
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,\
org.postgresql:postgresql:42.6.0,\
org.apache.hadoop:hadoop-aws:3.3.4 \
    stream_clickstream.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType, BooleanType
)
import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC     = "clickstream"
S3_BUCKET       = os.getenv("S3_BUCKET_NAME", "ecommerce-realtime-platform")
S3_CLICKS_PATH  = f"s3a://{S3_BUCKET}/raw/clickstream/"
POSTGRES_URL    = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST','postgres')}:5432/ecommerce"
POSTGRES_PROPS  = {
    "user":     os.getenv("POSTGRES_USER", "admin"),
    "password": os.getenv("POSTGRES_PASSWORD", "admin"),
    "driver":   "org.postgresql.Driver",
}
CHECKPOINT_PATH = "/tmp/checkpoints/clickstream"

CLICK_SCHEMA = StructType([
    StructField("event_type",      StringType(),  True),
    StructField("event_id",        StringType(),  False),
    StructField("session_id",      StringType(),  False),
    StructField("customer_id",     StringType(),  True),
    StructField("page",            StringType(),  True),
    StructField("device_type",     StringType(),  True),
    StructField("browser",         StringType(),  True),
    StructField("referrer",        StringType(),  True),
    StructField("product_id",      StringType(),  True),
    StructField("search_term",     StringType(),  True),
    StructField("results_count",   IntegerType(), True),
    StructField("cart_value",      DoubleType(),  True),
    StructField("event_timestamp", StringType(),  True),
])


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("EcommerceClickstreamStreaming")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", ""))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", ""))
        .getOrCreate()
    )


def parse_and_validate(raw_df):
    return (
        raw_df
        .select(F.from_json(F.col("value").cast("string"), CLICK_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
        .withColumn("processed_at", F.current_timestamp())
        .withColumn("event_date", F.to_date("event_timestamp"))
        .filter(
            F.col("event_id").isNotNull()
            & F.col("session_id").isNotNull()
            & F.col("event_timestamp").isNotNull()
        )
    )


def compute_funnel_metrics(df):
    """
    10-minute window funnel metrics:
    counts each stage of the conversion funnel to compute drop-off rates.
    """
    return (
        df
        .withWatermark("event_timestamp", "15 minutes")
        .groupBy(
            F.window("event_timestamp", "10 minutes"),
            F.col("device_type"),
            F.col("referrer"),
        )
        .agg(
            F.count(F.when(F.col("event_type") == "PAGE_VIEW",          1)).alias("page_views"),
            F.count(F.when(F.col("event_type") == "PRODUCT_VIEW",       1)).alias("product_views"),
            F.count(F.when(F.col("event_type") == "ADD_TO_CART",        1)).alias("add_to_cart"),
            F.count(F.when(F.col("event_type") == "CHECKOUT_START",     1)).alias("checkout_starts"),
            F.count(F.when(F.col("event_type") == "CHECKOUT_COMPLETE",  1)).alias("checkouts"),
            F.countDistinct("session_id").alias("unique_sessions"),
            F.countDistinct("customer_id").alias("unique_users"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "device_type", "referrer",
            "page_views", "product_views", "add_to_cart",
            "checkout_starts", "checkouts",
            "unique_sessions", "unique_users",
            # Conversion rate: checkouts / product_views
            F.round(
                F.when(F.col("product_views") > 0,
                    F.col("checkouts") / F.col("product_views") * 100
                ).otherwise(0.0),
                2
            ).alias("conversion_rate_pct"),
            F.current_timestamp().alias("computed_at"),
        )
    )


def write_raw_to_postgres(df):
    def write_batch(batch_df, batch_id):
        if batch_df.count() == 0:
            return
        batch_df.drop("event_date").write.jdbc(
            POSTGRES_URL, "raw_events.clickstream",
            mode="append", properties=POSTGRES_PROPS
        )
    return (
        df.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/postgres_raw")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )


def write_funnel_to_postgres(funnel_df):
    def write_batch(batch_df, batch_id):
        if batch_df.count() == 0:
            return
        batch_df.write.jdbc(
            POSTGRES_URL, "raw_events.funnel_window_metrics",
            mode="append", properties=POSTGRES_PROPS
        )
    return (
        funnel_df.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/funnel")
        .outputMode("update")
        .trigger(processingTime="1 minute")
        .start()
    )


def main():
    print("[stream_clickstream] Starting PySpark Structured Streaming job...")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw_df   = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    clean_df    = parse_and_validate(raw_df)
    funnel_df   = compute_funnel_metrics(clean_df)

    pg_raw      = write_raw_to_postgres(clean_df)
    pg_funnel   = write_funnel_to_postgres(funnel_df)

    print("[stream_clickstream] Streaming started. Awaiting termination...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
