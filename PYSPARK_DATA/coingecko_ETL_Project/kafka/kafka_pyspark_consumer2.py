"""
============================================================
PySpark Kafka Consumer
CoinGecko Live Streaming ETL
============================================================

1. Connect Spark to Kafka
2. Define JSON Schema
3. Parse Kafka Messages
4. Clean and Transform Data
5. Analyze and Validate Data
6. Save Results to Parquet

INSTALL : pip install pyspark
RUN     : python kafka/kafka_pyspark_consumer1.py

REQUIRES:
    docker-compose up -d
    python kafka/kafka_producer1.py
============================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType, IntegerType
)


# ============================================================
# PART 1 — CONFIGURATION
# ============================================================

KAFKA_BROKER = "localhost:9092"  # Kafka server address
KAFKA_TOPIC = "crypto-prices"  # Kafka topic name

TRIGGER_EVERY = "30 seconds"  # Process incoming data every 30 seconds

OUTPUT_PATH = "output/kafka_live_parquet"  # Directory for processed Parquet files
CHECKPOINT_PATH = "output/checkpoints/kafka_stream"  # Directory for streaming checkpoints


# ============================================================
# PART 2 — START SPARK SESSION
# ============================================================

# 1. PySpark Application -> SparkSession.builder
# 2. Maven Repository -> Downloads Kafka Connector JAR and dependencies
# 3. Spark Runtime -> Loads connector libraries
# 4. Kafka Connected ->  Spark can read Kafka topics

# Part                        	Meaning
# spark.jars.packages	      Spark ko external dependencies download karne ke liye batata hai
# org.apache.spark	          Maven group ID
# spark-sql-kafka-0-10_2.12   Kafka integration library
# 3.4.0	                      Connector library version

# Important: Ye Python package nahi hai. Ye Java/Scala ecosystem ki Maven dependency hai, jise PySpark automatically download kar sakta hai.

spark = (
    SparkSession.builder
    .appName("CoinGeckoKafkaETL")  # Set Spark application name
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0"  # Add Kafka connector dependency
    )
    .getOrCreate()  # Create or reuse Spark session
)

spark.sparkContext.setLogLevel("WARN")  # Display warning and error logs only

print("✓ Spark started")  # Display Spark started message


# ============================================================
# PART 3 — DEFINE SCHEMA
# ============================================================

coin_schema = StructType([
    StructField("id", StringType(), True),  # Cryptocurrency unique ID
    StructField("symbol", StringType(), True),  # Cryptocurrency symbol
    StructField("name", StringType(), True),  # Cryptocurrency name
    StructField("current_price", DoubleType(), True),  # Current coin price
    StructField("market_cap", LongType(), True),  # Total market capitalization
    StructField("market_cap_rank", IntegerType(), True),  # Market cap ranking
    StructField("total_volume", LongType(), True),  # Total trading volume
    StructField("high_24h", DoubleType(), True),  # Highest price in last 24 hours
    StructField("low_24h", DoubleType(), True),  # Lowest price in last 24 hours
    StructField("price_change_percentage_24h", DoubleType(), True),  # 24-hour price change
    StructField("circulating_supply", DoubleType(), True),  # Coins currently in circulation
    StructField("total_supply", DoubleType(), True),  # Total coin supply
    StructField("max_supply", DoubleType(), True),  # Maximum possible coin supply
    StructField("ath", DoubleType(), True),  # All-time high price
    StructField("ath_date", StringType(), True),  # Date of all-time high
    StructField("last_updated", StringType(), True),  # Last API update timestamp
    StructField("kafka_ingested_at", StringType(), True),  # Kafka ingestion timestamp
])

print("✓ Schema defined")  # Display schema creation message


# ============================================================
# PART 4 — CONNECT TO KAFKA (EXTRACT)
# ============================================================

raw_stream = (
    spark.readStream
    .format("kafka")  # Read data from Kafka
    .option("kafka.bootstrap.servers", KAFKA_BROKER)  # Connect to Kafka broker
    .option("subscribe", KAFKA_TOPIC)  # Subscribe to crypto-prices topic
    .option("startingOffsets", "latest")  # Read newly arriving messages
    .option("failOnDataLoss", "false")  # Continue if some Kafka data is unavailable
    .load()  # Load incoming Kafka records
)

# Kafka stores messages as key-value pairs; the value contains JSON data.

df_stream = (
    raw_stream.select(
        F.from_json(
            F.col("value").cast("string"),  # Convert Kafka binary value to string
            coin_schema  # Apply the predefined JSON schema
        ).alias("coin")  # Name the parsed JSON column
    )
    .select("coin.*")  # Extract all cryptocurrency columns
)

print("✓ Connected to Kafka topic — stream ready")  # Display Kafka connection message


# ============================================================
# PART 5 — PROCESS EACH BATCH
# CLEAN → TRANSFORM → ANALYZE → VALIDATE → LOAD
# ============================================================

def process_batch(batch_df, batch_id):  # Runs whenever a new micro-batch arrives

    count = batch_df.count()  # Count records in the current batch

    if count == 0:  # Check whether the batch is empty
        print(f"[Batch #{batch_id}] Empty — waiting for data...")  # Display empty batch message
        return  # Stop processing this batch

    print(f"\n{'=' * 55}")  # Print separator line
    print(f"[Batch #{batch_id}] {count} records from Kafka")  # Display batch ID and record count
    print(f"{'=' * 55}")  # Print separator line

    # --------------------------------------------------------
    # STEP 3 — DATA CLEANING
    # --------------------------------------------------------

    print("\n[STEP 3] DATA CLEANING")  # Display data cleaning heading

    df = batch_df.na.fill({
        "current_price": 0.0,  # Replace null prices with 0.0
        "market_cap": 0,  # Replace null market caps with 0
        "total_volume": 0,  # Replace null trading volumes with 0
        "circulating_supply": 0.0,  # Replace null circulating supply with 0.0
    })

    df = df.withColumn(
        "max_supply",
        F.coalesce(
            F.col("max_supply"),  # Keep existing maximum supply when available
            F.col("circulating_supply")  # Use circulating supply when maximum supply is null
        )
    )

    df = df.filter(F.col("current_price") > 0)  # Keep records with positive prices

    print(f"  After cleaning: {df.count()} records")  # Display records remaining after cleaning

    # --------------------------------------------------------
    # STEP 4 — DATA TRANSFORMATION
    # --------------------------------------------------------

    print("\n[STEP 4] DATA TRANSFORMATION")  # Display transformation heading

    df = (
        df.withColumn(
            "price_to_market_cap_ratio",
            F.col("current_price") / (F.col("market_cap") + 1)  # Calculate price-to-market-cap ratio
        )
        .withColumn(
            "volume_to_market_cap_ratio",
            F.col("total_volume") / (F.col("market_cap") + 1)  # Calculate volume-to-market-cap ratio
        )
    )

    df = df.withColumn(
        "market_cap_category",
        F.when(F.col("market_cap") >= 100_000_000_000, "Large Cap")  # Market cap >= 100 billion
         .when(F.col("market_cap") >= 10_000_000_000, "Mid Cap")  # Market cap >= 10 billion
         .when(F.col("market_cap") >= 1_000_000_000, "Small Cap")  # Market cap >= 1 billion
         .otherwise("Micro Cap")  # Remaining coins
    )

    df = df.withColumn(
        "price_change_category",
        F.when(F.col("price_change_percentage_24h") >= 5, "Strong Positive")  # Increase of 5% or more
         .when(F.col("price_change_percentage_24h") >= 0, "Positive")  # No change or positive change
         .when(F.col("price_change_percentage_24h") >= -5, "Negative")  # Decrease up to 5%
         .otherwise("Strong Negative")  # Decrease greater than 5%
    )

    df = df.withColumn(
        "processed_at",
        F.current_timestamp()  # Add Spark processing timestamp
    )

    df_transformed = df  # Store the transformed DataFrame

    # --------------------------------------------------------
    # STEP 5 — TOP 10 COINS BY MARKET CAP
    # --------------------------------------------------------

    print("\n[STEP 5] TOP 10 COINS (this batch):")  # Display top 10 heading

    top_10_df = (
        df_transformed
        .orderBy(F.desc("market_cap"))  # Sort by market cap from highest to lowest
        .select(
            "name",  # Cryptocurrency name
            "symbol",  # Cryptocurrency symbol
            "current_price",  # Current coin price
            "market_cap_category",  # Market cap category
            "price_change_category",  # Price change category
            "price_change_percentage_24h"  # 24-hour price change percentage
        )
        .limit(10)  # Select only the top 10 coins
    )

    top_10_df.show(truncate=False)  # Display top 10 results

    # --------------------------------------------------------
    # STEP 6 — MARKET CAP CATEGORY SUMMARY
    # --------------------------------------------------------

    print("\n[STEP 6] MARKET CAP CATEGORY SUMMARY:")  # Display summary heading

    category_summary = (
        df_transformed
        .groupBy("market_cap_category")  # Group coins by market cap category
        .agg(
            F.count("*").alias("coin_count"),  # Count coins in each category
            F.round(F.avg("current_price"), 2).alias("avg_price"),  # Calculate average price
            F.round(F.sum("market_cap") / 1e9, 2).alias("total_mcap_B")  # Calculate market cap in billions
        )
        .orderBy(F.desc("total_mcap_B"))  # Sort categories by total market cap
    )

    category_summary.show(truncate=False)  # Display category summary

    # --------------------------------------------------------
    # STEP 7 — DATA VALIDATION
    # --------------------------------------------------------

    print("\n[STEP 7] DATA VALIDATION:")  # Display validation heading

    neg_prices = df_transformed.filter(
        F.col("current_price") < 0  # Find negative prices
    ).count()  # Count negative-price records

    zero_mcap = df_transformed.filter( 
        (F.col("market_cap") == 0) &  # Check for zero market cap
        (F.col("current_price") > 0)  # Check for positive price
    ).count()  # Count records with zero market cap

    print(f"  Negative prices     : {neg_prices}")  # Display negative price count
    print(f"  Zero market cap     : {zero_mcap}")  # Display zero market cap count
    print(f"  Records after clean : {df_transformed.count()}")  # Display cleaned record count

    # --------------------------------------------------------
    # STEP 8 — SAVE DATA TO PARQUET
    # --------------------------------------------------------

    print(f"\n[STEP 8] SAVING BATCH #{batch_id} → {OUTPUT_PATH}")  # Display save location

    (
        df_transformed.write
        .mode("append")  # Append data without deleting existing files
        .partitionBy("market_cap_category")  # Create partitions by market cap category
        .parquet(OUTPUT_PATH)  # Save data in Parquet format
    )

    print(f"  ✓ Saved {df_transformed.count()} records")  # Display saved record count
    print(f"\n  Waiting {TRIGGER_EVERY} for next batch...\n")  # Display next batch waiting message


# ============================================================
# PART 6 — START STREAMING QUERY
# ============================================================

print("\n✓ Starting streaming query...")  # Display streaming startup message

print(f"  Topic      : {KAFKA_TOPIC}")  # Display Kafka topic
print(f"  Output     : {OUTPUT_PATH}")  # Display Parquet output path
print(f"  Checkpoint : {CHECKPOINT_PATH}")  # Display checkpoint path
print(f"  Trigger    : every {TRIGGER_EVERY}")  # Display trigger interval

print("  Press Ctrl+C to stop\n")  # Display stop instruction

query = (
    df_stream.writeStream
    .foreachBatch(process_batch)  # Run process_batch for every micro-batch
    .trigger(processingTime=TRIGGER_EVERY)  # Set micro-batch interval
    .option("checkpointLocation", CHECKPOINT_PATH)  # Enable checkpoint recovery
    .start()  # Start streaming query
)

try:
    query.awaitTermination()  # Keep the streaming application running

except KeyboardInterrupt:  # Handle Ctrl+C

    print("\n⏹ Stopped by user")  # Display stop message

    query.stop()  # Stop streaming query
    spark.stop()  # Stop Spark session

    print("✓ Spark closed cleanly")  # Display Spark shutdown message
