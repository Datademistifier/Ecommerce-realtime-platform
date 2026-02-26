"""
stream_orders.py
----------------
PySpark Structured Streaming job.

Reads order events from Kafka topic 'orders':
  1. Parses and enforces schema
  2. Applies data quality filters (rejects nulls, negative totals)
  3. Writes raw valid events to AWS S3 (data lake — Parquet, partitioned by date)
  4. Writes enriched events to PostgreSQL (raw_events schema)
  5. Computes 5-minute windowed revenue aggregates → PostgreSQL

Submit:
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,\
org.postgresql:postgresql:42.6.0,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    stream_orders.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType, TimestampType, BooleanType
)
import os

# ── Config ────────────────────────────────────────────────────
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC      = "orders"
S3_BUCKET        = os.getenv("S3_BUCKET_NAME", "ecommerce-realtime-platform")
S3_RAW_PATH      = f"s3a://{S3_BUCKET}/raw/orders/"
POSTGRES_URL     = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST','postgres')}:5432/ecommerce"
POSTGRES_PROPS   = {
    "user":   os.getenv("POSTGRES_USER", "admin"),
    "password": os.getenv("POSTGRES_PASSWORD", "admin"),
    "driver": "org.postgresql.Driver",
}
CHECKPOINT_PATH  = "/tmp/checkpoints/orders"

# ── Schema — enforces structure on raw JSON ───────────────────
ORDER_SCHEMA = StructType([
    StructField("event_type",      StringType(),    True),
    StructField("order_id",        StringType(),    False),
    StructField("customer_id",     StringType(),    False),
    StructField("product_id",      StringType(),    True),
    StructField("product_name",    StringType(),    True),
    StructField("category",        StringType(),    True),
    StructField("quantity",        IntegerType(),   True),
    StructField("unit_price",      DoubleType(),    True),
    StructField("order_total",     DoubleType(),    True),
    StructField("order_status",    StringType(),    True),
    StructField("shipping_state",  StringType(),    True),
    StructField("payment_method",  StringType(),    True),
    StructField("event_timestamp", StringType(),    True),
])


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("EcommerceOrdersStreaming")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH)
        # S3 / Hadoop AWS config
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", ""))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", ""))
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        # Kafka watermark
        .config("spark.sql.streaming.kafka.useDeprecatedOffsetFetching", "false")
        .getOrCreate()
    )


def read_from_kafka(spark: SparkSession):
    """Read raw bytes from Kafka topic."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 1000)   # backpressure control
        .load()
    )


def parse_events(raw_df):
    """
    Parse JSON value from Kafka, enforce schema,
    add processing metadata columns.
    """
    parsed = (
        raw_df
        .select(
            F.from_json(
                F.col("value").cast("string"),
                ORDER_SCHEMA
            ).alias("data"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
        )
        .select(
            "data.*",
            "kafka_timestamp",
            "kafka_partition",
            "kafka_offset",
        )
        # Parse event_timestamp string → proper timestamp
        .withColumn(
            "event_timestamp",
            F.to_timestamp(F.col("event_timestamp"))
        )
        # Add processing timestamp
        .withColumn("processed_at", F.current_timestamp())
        # Add date partition column for S3
        .withColumn("event_date", F.to_date(F.col("event_timestamp")))
    )
    return parsed


def apply_data_quality(df):
    """
    Filter out records that fail quality checks.
    Bad records are routed to a separate 'rejected' stream
    in a production system — here we log and drop them.
    """
    valid = df.filter(
        F.col("order_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & F.col("order_total").isNotNull()
        & (F.col("order_total") > 0)
        & F.col("quantity").isNotNull()
        & (F.col("quantity") > 0)
        & F.col("event_timestamp").isNotNull()
    )
    return valid


def write_to_s3(df, checkpoint_suffix="s3"):
    """
    Write raw validated events to S3 as Parquet,
    partitioned by event_date for efficient downstream querying.
    """
    return (
        df.writeStream
        .format("parquet")
        .option("path", S3_RAW_PATH)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/{checkpoint_suffix}")
        .partitionBy("event_date")
        .outputMode("append")
        .trigger(processingTime="30 seconds")  # micro-batch every 30s
        .start()
    )


def write_to_postgres(df, table: str, checkpoint_suffix="postgres"):
    """
    Write events to PostgreSQL using foreachBatch.
    foreachBatch gives us JDBC write control and
    allows upsert logic (INSERT ON CONFLICT) if needed.
    """
    def write_batch(batch_df, batch_id):
        if batch_df.count() == 0:
            return
        (
            batch_df
            .drop("event_date")   # not a postgres column
            .write
            .jdbc(
                url=POSTGRES_URL,
                table=table,
                mode="append",
                properties=POSTGRES_PROPS,
            )
        )
        print(f"[stream_orders] Batch {batch_id}: wrote {batch_df.count()} rows to {table}")

    return (
        df.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/{checkpoint_suffix}")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )


def compute_windowed_aggregates(df):
    """
    5-minute tumbling window aggregations on the streaming data.
    Produces real-time revenue and order count metrics.

    Uses watermark to handle late-arriving events (up to 10 minutes late).
    """
    return (
        df
        .withWatermark("event_timestamp", "10 minutes")
        .groupBy(
            F.window("event_timestamp", "5 minutes"),
            F.col("category"),
            F.col("shipping_state"),
        )
        .agg(
            F.count("order_id").alias("order_count"),
            F.sum("order_total").alias("total_revenue"),
            F.avg("order_total").alias("avg_order_value"),
            F.sum("quantity").alias("total_items_sold"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "category",
            "shipping_state",
            "order_count",
            F.round("total_revenue", 2).alias("total_revenue"),
            F.round("avg_order_value", 2).alias("avg_order_value"),
            "total_items_sold",
            "unique_customers",
            F.current_timestamp().alias("computed_at"),
        )
    )


def write_aggregates_to_postgres(agg_df, checkpoint_suffix="agg"):
    def write_batch(batch_df, batch_id):
        if batch_df.count() == 0:
            return
        batch_df.write.jdbc(
            url=POSTGRES_URL,
            table="raw_events.order_window_aggregates",
            mode="append",
            properties=POSTGRES_PROPS,
        )

    return (
        agg_df.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/{checkpoint_suffix}")
        .outputMode("update")   # update mode for windowed aggregates
        .trigger(processingTime="1 minute")
        .start()
    )


def main():
    print("[stream_orders] Starting PySpark Structured Streaming job...")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # 1. Read from Kafka
    raw_df = read_from_kafka(spark)

    # 2. Parse and validate
    parsed_df = parse_events(raw_df)
    clean_df  = apply_data_quality(parsed_df)

    # 3. Start all output streams
    s3_query       = write_to_s3(clean_df, "s3")
    pg_query       = write_to_postgres(clean_df, "raw_events.orders", "postgres")

    # 4. Windowed aggregates
    agg_df         = compute_windowed_aggregates(clean_df)
    agg_query      = write_aggregates_to_postgres(agg_df, "agg")

    print("[stream_orders] All streaming queries started. Awaiting termination...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
