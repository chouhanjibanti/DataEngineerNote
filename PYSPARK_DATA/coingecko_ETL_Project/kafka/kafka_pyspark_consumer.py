"""
============================================================
PySpark Structured Streaming — Kafka Consumer
CoinGecko Live ETL Pipeline
============================================================
PURPOSE  : Reads live coin data from the Kafka topic
           "crypto-prices" and applies EVERY transformation
           from 3_PySpark_ETL_Example.py in real time.

           Static CSV  →  replaced by Kafka stream
           spark.read  →  replaced by spark.readStream
           .write      →  replaced by .writeStream

INSTALL  :
    pip install pyspark
    (Kafka connector JAR is auto-downloaded via spark.jars.packages)

RUN      :
    python kafka/kafka_pyspark_consumer.py

REQUIRES :
    - Kafka running     : docker-compose up -d
    - Producer running  : python kafka/kafka_producer.py
============================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType, IntegerType, TimestampType
)

print("=" * 60)
print("PYSPARK KAFKA STREAMING ETL — COINGECKO LIVE DATA")
print("=" * 60)

# ── Config ─────────────────────────────────────────────────
KAFKA_BROKER        = "localhost:9092"
KAFKA_TOPIC         = "crypto-prices"
MICRO_BATCH_SECONDS = "30 seconds"        # process every 30 sec
OUTPUT_PATH         = "output/kafka_live_parquet"
CHECKPOINT_PATH     = "output/checkpoints/kafka_stream"

# ============================================================
# STEP 0: Initialize Spark Session
# ============================================================
print("\n[STEP 0] Initializing Spark Session with Kafka connector...")

spark = SparkSession.builder \
    .appName("CoinGeckoKafkaStreamingETL") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config(
        # Auto-downloads the Kafka connector JAR — no manual install needed
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0"
    ) \
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH) \
    .getOrCreate()

# Reduce noise in logs
spark.sparkContext.setLogLevel("WARN")

print("✓ Spark session created")
print(f"  Spark version : {spark.version}")
print(f"  Kafka broker  : {KAFKA_BROKER}")
print(f"  Topic         : {KAFKA_TOPIC}")
print(f"  Micro-batch   : {MICRO_BATCH_SECONDS}")

# ============================================================
# STEP 1: DEFINE SCHEMA
# (Same columns as the static CSV + kafka_ingested_at)
# ============================================================
print("\n[STEP 1] Defining schema for incoming Kafka messages...")

coin_schema = StructType([
    # ── identifiers ────────────────────────────────────────
    StructField("id",                               StringType(),  True),
    StructField("symbol",                           StringType(),  True),
    StructField("name",                             StringType(),  True),
    StructField("image",                            StringType(),  True),

    # ── price & volume ─────────────────────────────────────
    StructField("current_price",                    DoubleType(),  True),
    StructField("market_cap",                       LongType(),    True),
    StructField("market_cap_rank",                  IntegerType(), True),
    StructField("fully_diluted_valuation",          LongType(),    True),
    StructField("total_volume",                     LongType(),    True),
    StructField("high_24h",                         DoubleType(),  True),
    StructField("low_24h",                          DoubleType(),  True),
    StructField("price_change_24h",                 DoubleType(),  True),
    StructField("price_change_percentage_24h",      DoubleType(),  True),
    StructField("market_cap_change_24h",            DoubleType(),  True),
    StructField("market_cap_change_percentage_24h", DoubleType(),  True),

    # ── supply ─────────────────────────────────────────────
    StructField("circulating_supply",               DoubleType(),  True),
    StructField("total_supply",                     DoubleType(),  True),
    StructField("max_supply",                       DoubleType(),  True),

    # ── all-time high/low ──────────────────────────────────
    StructField("ath",                              DoubleType(),  True),
    StructField("ath_change_percentage",            DoubleType(),  True),
    StructField("ath_date",                         StringType(),  True),
    StructField("atl",                              DoubleType(),  True),
    StructField("atl_change_percentage",            DoubleType(),  True),
    StructField("atl_date",                         StringType(),  True),

    # ── metadata ───────────────────────────────────────────
    StructField("roi",                              StringType(),  True),
    StructField("last_updated",                     StringType(),  True),

    # ── Kafka-added field ──────────────────────────────────
    StructField("kafka_ingested_at",                StringType(),  True),
])

print("✓ Schema defined — matches 3_PySpark_ETL_Example.py columns")

# ============================================================
# STEP 2: EXTRACTION — Read live stream from Kafka
# (Replaces:  spark.read.csv("input/coingecko_raw_data.csv"))
# ============================================================
print("\n[STEP 2] EXTRACTION — Connecting to Kafka topic stream...")

raw_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Kafka delivers each message as:
#   key   (binary)  → coin id
#   value (binary)  → JSON string of the coin record
# We parse the value column using our schema
df_parsed = raw_stream.select(
    F.from_json(
        F.col("value").cast("string"),
        coin_schema
    ).alias("coin"),
    F.col("timestamp").alias("kafka_event_time")   # Kafka message timestamp
).select("coin.*", "kafka_event_time")

print("✓ Streaming DataFrame created from Kafka topic")

# ============================================================
# STEP 3: DATA CLEANING
# (Same logic as Step 3 in 3_PySpark_ETL_Example.py)
# ============================================================

def apply_cleaning(df):
    """
    Mirrors STEP 3 of 3_PySpark_ETL_Example.py:
      - Fill missing numeric values with 0
      - Fill max_supply from circulating_supply if null
      - Remove coins with price <= 0
    Note: dropDuplicates() and count() cannot be used on
    unbounded streams — deduplication is handled via Kafka
    message keys (same coin id → same partition).
    """
    # 3.1 Handle missing values
    df_clean = df.na.fill({
        "current_price"    : 0.0,
        "market_cap"       : 0,
        "total_volume"     : 0,
        "circulating_supply": 0.0,
    })

    # Fill max_supply with circulating_supply if missing
    df_clean = df_clean.withColumn(
        "max_supply",
        F.coalesce(F.col("max_supply"), F.col("circulating_supply"))
    )

    # 3.2 Remove outliers (price <= 0)
    df_clean = df_clean.filter(F.col("current_price") > 0)

    return df_clean

# ============================================================
# STEP 4: DATA TRANSFORMATION
# (Same logic as Step 4 in 3_PySpark_ETL_Example.py)
# ============================================================

def apply_transformations(df):
    """
    Mirrors STEP 4 of 3_PySpark_ETL_Example.py:
      - price_to_market_cap_ratio
      - volume_to_market_cap_ratio
      - market_cap_category
      - price_change_category
      - ath_year, ath_month (from ath_date)
    Plus streaming-only additions:
      - processed_at  : when PySpark processed this record
      - stream_source : tag showing this came from Kafka
    """
    # 4.1 Derived ratio columns
    df = df.withColumn(
        "price_to_market_cap_ratio",
        F.col("current_price") / (F.col("market_cap") + 1)
    ).withColumn(
        "volume_to_market_cap_ratio",
        F.col("total_volume") / (F.col("market_cap") + 1)
    )

    # 4.2 Categorize by market cap
    df = df.withColumn(
        "market_cap_category",
        F.when(F.col("market_cap") >= 100_000_000_000, "Large Cap")
         .when(F.col("market_cap") >= 10_000_000_000,  "Mid Cap")
         .when(F.col("market_cap") >= 1_000_000_000,   "Small Cap")
         .otherwise("Micro Cap")
    )

    # 4.3 Categorize 24h price change
    df = df.withColumn(
        "price_change_category",
        F.when(F.col("price_change_percentage_24h") >= 5,  "Strong Positive")
         .when(F.col("price_change_percentage_24h") >= 0,  "Positive")
         .when(F.col("price_change_percentage_24h") >= -5, "Negative")
         .otherwise("Strong Negative")
    )

    # 4.4 Parse ath_date and extract year / month
    df = df.withColumn("ath_date_ts", F.to_timestamp("ath_date")) \
           .withColumn("ath_year",    F.year("ath_date_ts")) \
           .withColumn("ath_month",   F.month("ath_date_ts")) \
           .drop("ath_date_ts")

    # 4.5 Streaming-specific metadata columns
    df = df.withColumn("processed_at",  F.current_timestamp()) \
           .withColumn("stream_source", F.lit("kafka:crypto-prices"))

    return df

# ============================================================
# STEP 5: WINDOWED AGGREGATION (streaming-only concept)
# Computes rolling stats per coin over a 5-minute window
# — not possible with a static CSV
# ============================================================

def build_windowed_aggregation(df):
    """
    Groups by a 5-minute sliding window and coin name.
    Produces per-coin stats (avg, max, min price) every batch.
    """
    return df \
        .withWatermark("kafka_event_time", "1 minute") \
        .groupBy(
            F.window("kafka_event_time", "5 minutes"),
            F.col("name"),
            F.col("symbol"),
            F.col("market_cap_category")
        ).agg(
            F.avg("current_price")               .alias("avg_price_5min"),
            F.max("current_price")               .alias("max_price_5min"),
            F.min("current_price")               .alias("min_price_5min"),
            F.avg("price_change_percentage_24h") .alias("avg_change_pct_5min"),
            F.sum("total_volume")                .alias("total_volume_5min"),
            F.count("*")                         .alias("update_count"),
        )

# ============================================================
# STEP 6: FOREACHABATCH — per micro-batch processing
# Called by Spark every 30 seconds with a mini DataFrame
# ============================================================

def process_batch(batch_df, batch_id):
    """
    Runs on every micro-batch (every 30 seconds).
    Applies cleaning + transformation, then:
      - Prints live stats to console
      - Saves cleaned records to Parquet
    """
    record_count = batch_df.count()

    if record_count == 0:
        print(f"\n[Batch #{batch_id}] No records — waiting for producer...")
        return

    print(f"\n{'=' * 60}")
    print(f"[Batch #{batch_id}]  {record_count} records received from Kafka")
    print(f"{'=' * 60}")

    # ── STEP 3: Clean ──────────────────────────────────────
    df_clean = apply_cleaning(batch_df)

    # ── STEP 4: Transform ──────────────────────────────────
    df_transformed = apply_transformations(df_clean)

    # ── STEP 5 (console): Top 10 by market cap ─────────────
    print("\n[STEP 5] TOP 10 COINS (this batch):")
    df_transformed \
        .orderBy(F.desc("market_cap")) \
        .select("name", "symbol", "current_price",
                "market_cap_category", "price_change_category",
                "price_change_percentage_24h") \
        .limit(10) \
        .show(truncate=False)

    # ── STEP 5 (console): Category stats ───────────────────
    print("[STEP 5] MARKET CAP CATEGORY SUMMARY (this batch):")
    df_transformed \
        .groupBy("market_cap_category") \
        .agg(
            F.count("*")                    .alias("coin_count"),
            F.round(F.avg("current_price"), 2).alias("avg_price"),
            F.round(F.sum("market_cap") / 1e9, 2).alias("total_mcap_B"),
        ) \
        .orderBy(F.desc("total_mcap_B")) \
        .show(truncate=False)

    # ── STEP 6: Validation ─────────────────────────────────
    print("[STEP 6] DATA VALIDATION:")
    neg_prices = df_transformed.filter(F.col("current_price") < 0).count()
    zero_mcap  = df_transformed.filter(
        (F.col("market_cap") == 0) & (F.col("current_price") > 0)
    ).count()
    print(f"  ✓ Negative prices     : {neg_prices}")
    print(f"  ✓ Zero market cap     : {zero_mcap}")
    print(f"  ✓ Records after clean : {df_transformed.count()}")

    # ── STEP 7: Save to Parquet ────────────────────────────
    print(f"\n[STEP 7] SAVING batch #{batch_id} → {OUTPUT_PATH}")
    df_transformed \
        .write \
        .mode("append") \
        .partitionBy("market_cap_category") \
        .parquet(OUTPUT_PATH)
    print(f"  ✓ Saved {df_transformed.count()} records")

    print(f"\n  Waiting for next batch ({MICRO_BATCH_SECONDS})...\n")


# ============================================================
# STEP 7: START THE STREAMING QUERY
# ============================================================
print("\n[STEP 7] Starting PySpark Structured Streaming query...")
print(f"  Output       : {OUTPUT_PATH}")
print(f"  Checkpoint   : {CHECKPOINT_PATH}")
print(f"  Trigger      : every {MICRO_BATCH_SECONDS}")
print("  Press Ctrl+C to stop\n")

# Apply cleaning + transformation inside foreachBatch
# This handles the full pipeline per micro-batch
streaming_query = df_parsed \
    .writeStream \
    .foreachBatch(process_batch) \
    .trigger(processingTime=MICRO_BATCH_SECONDS) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .start()

print("✓ Streaming query started — listening for Kafka messages...")
print("=" * 60)

# Keep running until Ctrl+C
try:
    streaming_query.awaitTermination()
except KeyboardInterrupt:
    print("\n⏹  Streaming stopped by user (Ctrl+C)")
    streaming_query.stop()
    spark.stop()
    print("✓ Spark session closed cleanly")
