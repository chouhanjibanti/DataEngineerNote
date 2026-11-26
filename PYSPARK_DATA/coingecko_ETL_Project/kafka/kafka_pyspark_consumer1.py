# """
# ============================================================
# PySpark Kafka Consumer — 
# CoinGecko Live Streaming ETL
# ============================================================
# WHAT THIS FILE DOES (5 things only):
#   1. Connect Spark to Kafka topic "crypto-prices"
#   2. Define the schema (what columns to expect)
#   3. Parse JSON messages from Kafka
#   4. Clean + Transform every 30 seconds (foreachBatch)
#   5. Save results to Parquet

# INSTALL  : pip install pyspark
# RUN      : python kafka/kafka_pyspark_consumer1.py
# REQUIRES : Kafka running  →  docker-compose up -d
#            Producer running  →  python kafka/kafka_producer1.py
# ============================================================
# """

# from pyspark.sql import SparkSession
# from pyspark.sql import functions as F
# from pyspark.sql.types import (
#     StructType, StructField,
#     StringType, DoubleType, LongType, IntegerType
# )

# # ============================================================
# # PART 1 — CONFIGURATION
# # ============================================================

# KAFKA_BROKER    = "localhost:9092"    # where Kafka is running
# KAFKA_TOPIC     = "crypto-prices"    # topic to read from
# TRIGGER_EVERY   = "30 seconds"       # process a batch every 30 sec
# OUTPUT_PATH     = "output/kafka_live_parquet"
# CHECKPOINT_PATH = "output/checkpoints/kafka_stream"

# # ============================================================
# # PART 2 — START SPARK SESSION
# # Key difference from batch:
# #   .config("spark.jars.packages", ...) downloads Kafka connector
# #   No manual JAR download needed
# # ============================================================

# spark = SparkSession.builder \
#     .appName("CoinGeckoKafkaETL") \
#     .config(
#         "spark.jars.packages",
#         "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0"
#         #  ↑ This JAR lets PySpark talk to Kafka — auto downloaded
#     ) \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")   # reduce noisy logs
# print("✓ Spark started")

# # ============================================================
# # PART 3 — DEFINE SCHEMA
# # Kafka sends raw bytes — PySpark needs to know the structure
# # This matches the columns from coingecko_raw_data.csv
# # ============================================================

# coin_schema = StructType([
#     StructField("id",                           StringType(),  True),
#     StructField("symbol",                       StringType(),  True),
#     StructField("name",                         StringType(),  True),
#     StructField("current_price",                DoubleType(),  True),
#     StructField("market_cap",                   LongType(),    True),
#     StructField("market_cap_rank",              IntegerType(), True),
#     StructField("total_volume",                 LongType(),    True),
#     StructField("high_24h",                     DoubleType(),  True),
#     StructField("low_24h",                      DoubleType(),  True),
#     StructField("price_change_percentage_24h",  DoubleType(),  True),
#     StructField("circulating_supply",           DoubleType(),  True),
#     StructField("total_supply",                 DoubleType(),  True),
#     StructField("max_supply",                   DoubleType(),  True),
#     StructField("ath",                          DoubleType(),  True),
#     StructField("ath_date",                     StringType(),  True),
#     StructField("last_updated",                 StringType(),  True),
#     StructField("kafka_ingested_at",            StringType(),  True),
# ])

# print("✓ Schema defined")

# # ============================================================
# # PART 4 — CONNECT TO KAFKA (EXTRACT)
# # Key difference vs batch:
# #   BATCH   →  spark.read.csv("input/coingecko_raw_data.csv")
# #   STREAM  →  spark.readStream.format("kafka")
# #
# # Kafka message structure:
# #   key   = coin id  (bytes)
# #   value = full JSON coin record (bytes)  ← we only need this
# # ============================================================

# raw_stream = spark \
#     .readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", KAFKA_BROKER) \
#     .option("subscribe", KAFKA_TOPIC) \
#     .option("startingOffsets", "latest") \
#     .option("failOnDataLoss", "false") \
#     .load()
# # failOnDataLoss=false → if Kafka restarts/resets offsets, don't crash

# # Parse the JSON value column using our schema
# df_stream = raw_stream.select(
#     F.from_json(
#         F.col("value").cast("string"),   # bytes → string → JSON
#         coin_schema                       # apply our structure
#     ).alias("coin")
# ).select("coin.*")                        # flatten: coin.name → name

# print("✓ Connected to Kafka topic — stream ready")

# # ============================================================
# # PART 5 — PROCESS EACH BATCH (TRANSFORM + LOAD)
# # foreachBatch() runs this function every 30 seconds
# # batch_df = a normal PySpark DataFrame of that 30-sec window
# # batch_id = auto-incrementing batch number (0, 1, 2, ...)
# # ============================================================

# def process_batch(batch_df, batch_id):

#     count = batch_df.count()
#     if count == 0:
#         print(f"[Batch #{batch_id}] Empty — waiting for data...")
#         return

#     print(f"\n{'=' * 55}")
#     print(f"[Batch #{batch_id}]  {count} records from Kafka")
#     print(f"{'=' * 55}")

#     # ── STEP 3: CLEAN ──────────────────────────────────────
#     # Same as Step 3 in 3_PySpark_ETL_Example.py
#     df = batch_df.na.fill({
#         "current_price"    : 0.0,
#         "market_cap"       : 0,
#         "total_volume"     : 0,
#         "circulating_supply": 0.0,
#     })
#     df = df.withColumn(
#         "max_supply",
#         F.coalesce(F.col("max_supply"), F.col("circulating_supply"))
#     )
#     df = df.filter(F.col("current_price") > 0)   # remove invalid prices
#     print(f"  After cleaning: {df.count()} records")

#     # ── STEP 4: TRANSFORM ──────────────────────────────────
#     # Same logic as Step 4 in 3_PySpark_ETL_Example.py

#     # Derived ratio columns
#     df = df.withColumn(
#         "price_to_market_cap_ratio",
#         F.col("current_price") / (F.col("market_cap") + 1)
#     ).withColumn(
#         "volume_to_market_cap_ratio",
#         F.col("total_volume") / (F.col("market_cap") + 1)
#     )

#     # Categorize by market cap size
#     df = df.withColumn(
#         "market_cap_category",
#         F.when(F.col("market_cap") >= 100_000_000_000, "Large Cap")
#          .when(F.col("market_cap") >= 10_000_000_000,  "Mid Cap")
#          .when(F.col("market_cap") >= 1_000_000_000,   "Small Cap")
#          .otherwise("Micro Cap")
#     )

#     # Categorize 24h price change direction
#     df = df.withColumn(
#         "price_change_category",
#         F.when(F.col("price_change_percentage_24h") >= 5,  "Strong Positive")
#          .when(F.col("price_change_percentage_24h") >= 0,  "Positive")
#          .when(F.col("price_change_percentage_24h") >= -5, "Negative")
#          .otherwise("Strong Negative")
#     )

#     # Add a timestamp of when PySpark processed this record
#     df = df.withColumn("processed_at", F.current_timestamp())

#     # ── STEP 5: SHOW RESULTS IN CONSOLE ────────────────────
#     print("\n  TOP 10 COINS (this batch):")
#     df.orderBy(F.desc("market_cap")) \
#       .select("name", "symbol", "current_price",
#               "market_cap_category", "price_change_category") \
#       .limit(10) \
#       .show(truncate=False)

#     # ── STEP 6: SAVE TO PARQUET ────────────────────────────
#     # Same as .write in batch, but mode="append" (never overwrite)
#     df.write \
#       .mode("append") \
#       .partitionBy("market_cap_category") \
#       .parquet(OUTPUT_PATH)

#     print(f"  ✓ Saved to {OUTPUT_PATH}")
#     print(f"  Waiting {TRIGGER_EVERY} for next batch...\n")


# # ============================================================
# # PART 6 — START THE STREAMING QUERY
# # Key difference vs batch:
# #   BATCH   →  df.write.mode("overwrite").csv(...)
# #   STREAM  →  df.writeStream.foreachBatch(...).start()
# #
# # .foreachBatch → calls process_batch() every 30 seconds
# # .trigger      → controls how often batches are processed
# # .start()      → begins the stream (runs forever)
# # .awaitTermination() → keeps the script alive (Ctrl+C to stop)
# # ============================================================

# print("\n✓ Starting streaming query...")
# print(f"  Topic      : {KAFKA_TOPIC}")
# print(f"  Output     : {OUTPUT_PATH}")
# print(f"  Checkpoint : {CHECKPOINT_PATH}")
# print(f"  Trigger    : every {TRIGGER_EVERY}")
# print("  Press Ctrl+C to stop\n")

# query = df_stream \
#     .writeStream \
#     .foreachBatch(process_batch) \
#     .trigger(processingTime=TRIGGER_EVERY) \
#     .option("checkpointLocation", CHECKPOINT_PATH) \
#     .start()

# try:
#     query.awaitTermination()
# except KeyboardInterrupt:
#     print("\n⏹  Stopped by user")
#     query.stop()
#     spark.stop()
#     print("✓ Spark closed cleanly")


# ===================================
# ===================================



# """
# ============================================================
# PySpark Kafka Consumer
# CoinGecko Live Streaming ETL
# ============================================================

# WHAT THIS FILE DOES:

# 1. Connect Spark to Kafka topic "crypto-prices"
# 2. Define schema for incoming JSON records
# 3. Parse JSON messages from Kafka
# 4. Clean incoming cryptocurrency data
# 5. Transform data and create derived columns
# 6. Display Top 10 coins by market cap
# 7. Generate market cap category summary
# 8. Validate data quality
# 9. Save processed data into partitioned Parquet files

# INSTALL:
#     pip install pyspark==3.4.0

# RUN:
#     python kafka/kafka_pyspark_consumer1.py

# REQUIRES:
#     Kafka running:
#         docker-compose up -d

#     Producer running:
#         python kafka/kafka_producer1.py

# ============================================================
# """

# from pyspark.sql import SparkSession
# from pyspark.sql import functions as F

# from pyspark.sql.types import ( StructType,StructField,StringType,DoubleType,LongType,IntegerType)

# # ============================================================
# # PART 1 — CONFIGURATION
# # ============================================================

# KAFKA_BROKER = "localhost:9092"
# KAFKA_TOPIC = "crypto-prices"

# TRIGGER_EVERY = "30 seconds"
# OUTPUT_PATH = "output/kafka_live_parquet"
# CHECKPOINT_PATH = "output/checkpoints/kafka_stream"

# # ============================================================
# # PART 2 — START SPARK SESSION
# # ============================================================

# spark = SparkSession.builder \
#     .appName("CoinGeckoKafkaETL") \
#     .config(
#         "spark.jars.packages",
#         "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0"
#     ) \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")

# print("\n" + "=" * 60)
# print("       COINGECKO KAFKA STREAMING ETL STARTED")
# print("=" * 60)

# print("✓ Spark session started")
# # ============================================================
# # PART 3 — DEFINE JSON SCHEMA
# # ============================================================

# coin_schema = StructType([

#     StructField("id", StringType(), True),
#     StructField("symbol", StringType(), True),
#     StructField("name", StringType(), True),
#     StructField("current_price", DoubleType(), True),
#     StructField("market_cap", LongType(), True),
#     StructField("market_cap_rank", IntegerType(), True),
#     StructField("total_volume", LongType(), True),
#     StructField("high_24h", DoubleType(), True),
#     StructField("low_24h", DoubleType(), True),
#     StructField("price_change_percentage_24h",DoubleType(),True),
#     StructField("circulating_supply", DoubleType(), True),
#     StructField("total_supply", DoubleType(), True),
#     StructField("max_supply", DoubleType(), True),
#     StructField("ath", DoubleType(), True),
#     StructField("ath_date", StringType(), True),
#     StructField("last_updated", StringType(), True),
#     StructField("kafka_ingested_at", StringType(), True),
#     # Stores malformed JSON information
#     StructField("_corrupt_record", StringType(), True)
# ])
# print("✓ Coin schema defined")

# # ============================================================
# # PART 4 — CONNECT TO KAFKA (EXTRACT)
# # ============================================================

# raw_stream = spark \
#     .readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", KAFKA_BROKER) \
#     .option("subscribe", KAFKA_TOPIC) \
#     .option("startingOffsets", "latest") \
#     .option("failOnDataLoss", "false") \
#     .load()

# print("✓ Connected to Kafka")

# # ============================================================
# # PART 5 — PARSE JSON MESSAGES
# # ============================================================

# # Kafka value arrives as bytes.
# # Convert bytes into string and parse JSON.

# parsed_stream = raw_stream.select(

#     F.from_json(
#         F.col("value").cast("string"),
#         coin_schema,
#         {"mode": "PERMISSIVE"}
#     ).alias("coin")

# )

# # Identify malformed JSON before flattening the structure.

# df_stream = parsed_stream \
#     .withColumn(
#         "_is_malformed",
#         F.col("coin").isNull() |
#         F.col("coin._corrupt_record").isNotNull()
#     ) \
#     .select(
#         "coin.*",
#         "_is_malformed"
#     )

# print("✓ JSON parsing completed")
# print("✓ Streaming DataFrame ready")

# # ============================================================
# # PART 6 — PROCESS EACH BATCH
# #
# # Every 30 seconds foreachBatch() calls this function.
# #
# # batch_df = Normal PySpark DataFrame
# # batch_id = Unique micro-batch ID within this checkpoint
# #
# # ETL:
# # CLEAN → TRANSFORM → ANALYZE → VALIDATE → LOAD
# # ============================================================

# def process_batch(batch_df, batch_id):

#     # --------------------------------------------------------
#     # STEP 0 — CHECK INCOMING DATA
#     # --------------------------------------------------------

#     total_records = batch_df.count()

#     if total_records == 0:

#         print(
#             f"\n[Batch #{batch_id}] "
#             "Empty — waiting for data..."
#         )
#         return

#     print("\n" + "=" * 60)

#     print(
#         f"[BATCH #{batch_id}] "
#         f"RECEIVED {total_records} RECORDS"
#     )

#     print("=" * 60)

#     # --------------------------------------------------------
#     # STEP 1 — DATA QUALITY CHECK BEFORE CLEANING
#     # --------------------------------------------------------

#     print("\n[STEP 1] INITIAL DATA QUALITY CHECK")

#     malformed_records = batch_df.filter(
#         F.col("_is_malformed")
#     ).count()

#     print(f"  Total incoming records : {total_records}")

#     print(f"  Malformed JSON records : {malformed_records}")

#     # Remove malformed JSON records

#     df = batch_df \
#         .filter(~F.col("_is_malformed")) \
#         .drop("_is_malformed", "_corrupt_record")


#     # Check invalid prices before cleaning

#     negative_prices = df.filter(
#         F.col("current_price") < 0
#     ).count()

#     null_prices = df.filter(
#         F.col("current_price").isNull()
#     ).count()

#     zero_market_cap = df.filter(
#         F.col("market_cap") == 0
#     ).count()

#     print(f"  Negative prices       : {negative_prices}")

#     print(f"  NULL prices           : {null_prices}")

#     print(f"  Zero market cap       : {zero_market_cap}")

#     # --------------------------------------------------------
#     # STEP 2 — DATA CLEANING
#     # --------------------------------------------------------

#     print("\n[STEP 2] DATA CLEANING")

#     # Replace NULL values with default values
#     df = df.na.fill({

#         "current_price": 0.0,
#         "market_cap": 0,
#         "total_volume": 0,
#         "circulating_supply": 0.0
#     })


#     # If max_supply is NULL, use circulating_supply
#     df = df.withColumn(

#         "max_supply",
#         F.coalesce(
#             F.col("max_supply"),
#             F.col("circulating_supply")
#         )

#     )


#     # Remove invalid prices
#     df = df.filter(
#         F.col("current_price") > 0
#     )

#     cleaned_count = df.count()

#     removed_records = (
#         total_records -
#         malformed_records -
#         cleaned_count
#     )

#     print(f"  Incoming records : {total_records}")

#     print(f"  Malformed JSON   : {malformed_records}")

#     print(f"  After cleaning   : {cleaned_count}")

#     print(f"  Removed records  : {removed_records}")


#     # If no valid data remains, skip this batch

#     if cleaned_count == 0:

#         print(
#             "\nNo valid records remaining. "
#             "Skipping this batch."
#         )

#         return

#     # --------------------------------------------------------
#     # STEP 3 — DATA TRANSFORMATION
#     # --------------------------------------------------------

#     print("\n[STEP 3] DATA TRANSFORMATION")


#     # 3.1 Calculate price-to-market-cap ratio

#     df = df.withColumn(

#         "price_to_market_cap_ratio",

#         F.col("current_price") /
#         (F.col("market_cap") + 1)

#     )

#     # 3.2 Calculate volume-to-market-cap ratio

#     df = df.withColumn(

#         "volume_to_market_cap_ratio",

#         F.col("total_volume") /
#         (F.col("market_cap") + 1)

#     )

#     # 3.3 Categorize coins based on market capitalization

#     df = df.withColumn(

#         "market_cap_category",

#         F.when(
#             F.col("market_cap") >= 100_000_000_000,
#             "Large Cap"
#         ).when(
#             F.col("market_cap") >= 10_000_000_000,
#             "Mid Cap"
#         ).when(
#             F.col("market_cap") >= 1_000_000_000,
#             "Small Cap"
#         ).otherwise("Micro Cap")
#     )


#     # 3.4 Categorize 24-hour price movement

#     df = df.withColumn(

#         "price_change_category",

#         F.when(
#             F.col("price_change_percentage_24h").isNull(),
#             "Unknown"
#         ).when(
#             F.col("price_change_percentage_24h") >= 5,
#             "Strong Positive"
#         ).when(
#             F.col("price_change_percentage_24h") >= 0,
#             "Positive"
#         ).when(
#             F.col("price_change_percentage_24h") >= -5,
#             "Negative"
#         ).otherwise("Strong Negative")
#     )

#     # 3.5 Add Spark processing timestamp

#     df = df.withColumn(
#         "processed_at",
#         F.current_timestamp()
#     )


#     # Cache DataFrame because multiple actions use it

#     df_transformed = df.cache()

#     print("✓ Derived columns created successfully")


#     # --------------------------------------------------------
#     # STEP 4 — TOP 10 COINS BY MARKET CAP
#     # --------------------------------------------------------

#     print("\n[STEP 4] TOP 10 COINS BY MARKET CAP")

#     df_transformed \
#         .orderBy(F.desc("market_cap")) \
#         .select(

#             "name",

#             "symbol",

#             "current_price",

#             "market_cap",

#             "market_cap_category",

#             "price_change_category",

#             "price_change_percentage_24h"

#         ) \
#         .limit(10) \
#         .show(truncate=False)


#     # --------------------------------------------------------
#     # STEP 5 — MARKET CAP CATEGORY SUMMARY
#     # --------------------------------------------------------

#     print("\n[STEP 5] MARKET CAP CATEGORY SUMMARY")


#     df_transformed \
#         .groupBy("market_cap_category") \
#         .agg(

#             F.count("*").alias("coin_count"),

#             F.round(
#                 F.avg("current_price"), 2
#             ).alias("avg_price"),

#             F.round(
#                 F.sum("market_cap") / 1e9, 2
#             ).alias("total_mcap_B")

#         ) \
#         .orderBy(F.desc("total_mcap_B")) \
#         .show(truncate=False)


#     # --------------------------------------------------------
#     # STEP 6 — FINAL DATA VALIDATION
#     # --------------------------------------------------------

#     print("\n[STEP 6] FINAL DATA VALIDATION")


#     # Check negative prices remaining after cleaning

#     remaining_negative_prices = df_transformed.filter(

#         F.col("current_price") < 0

#     ).count()


#     # Check zero market capitalization

#     remaining_zero_mcap = df_transformed.filter(

#         (F.col("market_cap") == 0) &
#         (F.col("current_price") > 0)

#     ).count()


#     # Check NULL prices remaining

#     remaining_null_prices = df_transformed.filter(

#         F.col("current_price").isNull()

#     ).count()


#     print(
#         f"  Negative prices after cleaning : "
#         f"{remaining_negative_prices}"
#     )

#     print(
#         f"  Zero market cap records        : "
#         f"{remaining_zero_mcap}"
#     )

#     print(
#         f"  NULL prices after cleaning     : "
#         f"{remaining_null_prices}"
#     )

#     print(
#         f"  Valid records                  : "
#         f"{cleaned_count}"
#     )


#     # --------------------------------------------------------
#     # STEP 7 — SAVE DATA INTO PARQUET
#     # --------------------------------------------------------

#     print("\n[STEP 7] SAVING DATA INTO PARQUET")


#     df_transformed \
#         .write \
#         .mode("append") \
#         .partitionBy("market_cap_category") \
#         .parquet(OUTPUT_PATH)


#     print(f"  Batch ID      : {batch_id}")

#     print(f"  Saved records : {cleaned_count}")

#     print(f"  Output path   : {OUTPUT_PATH}")

#     print("✓ Parquet storage completed")


#     # Release cached DataFrame

#     df_transformed.unpersist()


#     # --------------------------------------------------------
#     # BATCH COMPLETED
#     # --------------------------------------------------------

#     print("\n" + "=" * 60)

#     print(f"BATCH #{batch_id} COMPLETED SUCCESSFULLY")

#     print("=" * 60)

#     print(
#         f"Waiting for next batch "
#         f"({TRIGGER_EVERY})...\n"
#     )


# # ============================================================
# # PART 7 — START STREAMING QUERY
# # ============================================================

# print("\n" + "=" * 60)

# print("STARTING KAFKA STREAMING QUERY")

# print("=" * 60)

# print(f"Kafka Broker : {KAFKA_BROKER}")

# print(f"Kafka Topic  : {KAFKA_TOPIC}")

# print(f"Output Path  : {OUTPUT_PATH}")

# print(f"Checkpoint   : {CHECKPOINT_PATH}")

# print(f"Trigger      : {TRIGGER_EVERY}")

# print("\nPress CTRL+C to stop the application\n")


# query = None


# try:

#     query = df_stream \
#         .writeStream \
#         .foreachBatch(process_batch) \
#         .trigger(
#             processingTime=TRIGGER_EVERY
#         ) \
#         .option(
#             "checkpointLocation",
#             CHECKPOINT_PATH
#         ) \
#         .start()


#     # Keep Spark application running

#     query.awaitTermination()


# except KeyboardInterrupt:

#     print("\nStopping stream — user interrupted execution.")


# finally:

#     # Stop streaming query

#     if query is not None:

#         if query.isActive:

#             query.stop()

#             print("✓ Streaming query stopped")


#     # Close Spark session

#     spark.stop()

#     print("✓ Spark session closed")

#     print("\nCOINGECKO KAFKA ETL STOPPED")

# =======================================
# ======================================
# ======================================



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

INSTALL  : pip install pyspark
RUN      : python kafka/kafka_pyspark_consumer1.py

REQUIRES :
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

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "crypto-prices"

TRIGGER_EVERY = "30 seconds"

OUTPUT_PATH = "output/kafka_live_parquet"
CHECKPOINT_PATH = "output/checkpoints/kafka_stream"


# ============================================================
# PART 2 — START SPARK SESSION
# ============================================================

spark = SparkSession.builder \
    .appName("CoinGeckoKafkaETL") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("✓ Spark started")


# ============================================================
# PART 3 — DEFINE SCHEMA
# ============================================================

coin_schema = StructType([
    StructField("id", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("name", StringType(), True),
    StructField("current_price", DoubleType(), True),
    StructField("market_cap", LongType(), True),
    StructField("market_cap_rank", IntegerType(), True),
    StructField("total_volume", LongType(), True),
    StructField("high_24h", DoubleType(), True),
    StructField("low_24h", DoubleType(), True),
    StructField("price_change_percentage_24h", DoubleType(), True),
    StructField("circulating_supply", DoubleType(), True),
    StructField("total_supply", DoubleType(), True),
    StructField("max_supply", DoubleType(), True),
    StructField("ath", DoubleType(), True),
    StructField("ath_date", StringType(), True),
    StructField("last_updated", StringType(), True),
    StructField("kafka_ingested_at", StringType(), True),
])

print("✓ Schema defined")


# ============================================================
# PART 4 — CONNECT TO KAFKA (EXTRACT)
# ============================================================

raw_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Kafka bytes → String → JSON → DataFrame

df_stream = raw_stream.select(
    F.from_json(
        F.col("value").cast("string"),
        coin_schema
    ).alias("coin")
).select("coin.*")

print("✓ Connected to Kafka topic — stream ready")


# ============================================================
# PART 5 — PROCESS EACH BATCH
# CLEAN → TRANSFORM → ANALYZE → VALIDATE → LOAD
# ============================================================

def process_batch(batch_df, batch_id):

    count = batch_df.count()

    if count == 0:
        print(f"[Batch #{batch_id}] Empty — waiting for data...")
        return

    print(f"\n{'=' * 55}")
    print(f"[Batch #{batch_id}] {count} records from Kafka")
    print(f"{'=' * 55}")


    # --------------------------------------------------------
    # STEP 3 — DATA CLEANING
    # --------------------------------------------------------

    print("\n[STEP 3] DATA CLEANING")

    df = batch_df.na.fill({
        "current_price": 0.0,
        "market_cap": 0,
        "total_volume": 0,
        "circulating_supply": 0.0,
    })

    df = df.withColumn(
        "max_supply",
        F.coalesce(
            F.col("max_supply"),
            F.col("circulating_supply")
        )
    )

    # Remove invalid prices
    df = df.filter(F.col("current_price") > 0)

    print(f"  After cleaning: {df.count()} records")


    # --------------------------------------------------------
    # STEP 4 — DATA TRANSFORMATION
    # --------------------------------------------------------

    print("\n[STEP 4] DATA TRANSFORMATION")

    # Calculate derived ratios
    df = df.withColumn(
        "price_to_market_cap_ratio",
        F.col("current_price") / (F.col("market_cap") + 1)
    ).withColumn(
        "volume_to_market_cap_ratio",
        F.col("total_volume") / (F.col("market_cap") + 1)
    )

    # Categorize by market cap
    df = df.withColumn(
        "market_cap_category",
        F.when(F.col("market_cap") >= 100_000_000_000, "Large Cap")
         .when(F.col("market_cap") >= 10_000_000_000, "Mid Cap")
         .when(F.col("market_cap") >= 1_000_000_000, "Small Cap")
         .otherwise("Micro Cap")
    )

    # Categorize 24-hour price change
    df = df.withColumn(
        "price_change_category",
        F.when(F.col("price_change_percentage_24h") >= 5, "Strong Positive")
         .when(F.col("price_change_percentage_24h") >= 0, "Positive")
         .when(F.col("price_change_percentage_24h") >= -5, "Negative")
         .otherwise("Strong Negative")
    )

    # Add processing timestamp
    df = df.withColumn(
        "processed_at",
        F.current_timestamp()
    )

    # Use the transformed DataFrame for remaining steps
    df_transformed = df


    # --------------------------------------------------------
    # STEP 5 — TOP 10 COINS BY MARKET CAP
    # --------------------------------------------------------

    print("\n[STEP 5] TOP 10 COINS (this batch):")

    df_transformed \
        .orderBy(F.desc("market_cap")) \
        .select(
            "name",
            "symbol",
            "current_price",
            "market_cap_category",
            "price_change_category",
            "price_change_percentage_24h"
        ) \
        .limit(10) \
        .show(truncate=False)


    # --------------------------------------------------------
    # STEP 6 — MARKET CAP CATEGORY SUMMARY
    # --------------------------------------------------------

    print("\n[STEP 6] MARKET CAP CATEGORY SUMMARY:")

    df_transformed \
        .groupBy("market_cap_category") \
        .agg(
            F.count("*").alias("coin_count"),
            F.round(F.avg("current_price"), 2).alias("avg_price"),
            F.round(F.sum("market_cap") / 1e9, 2).alias("total_mcap_B")
        ) \
        .orderBy(F.desc("total_mcap_B")) \
        .show(truncate=False)


    # --------------------------------------------------------
    # STEP 7 — DATA VALIDATION
    # --------------------------------------------------------

    print("\n[STEP 7] DATA VALIDATION:")

    neg_prices = df_transformed.filter(
        F.col("current_price") < 0
    ).count()

    zero_mcap = df_transformed.filter(
        (F.col("market_cap") == 0) &
        (F.col("current_price") > 0)
    ).count()

    print(f"  Negative prices     : {neg_prices}")
    print(f"  Zero market cap     : {zero_mcap}")
    print(f"  Records after clean : {df_transformed.count()}")


    # --------------------------------------------------------
    # STEP 8 — SAVE DATA TO PARQUET
    # --------------------------------------------------------

    print(f"\n[STEP 8] SAVING BATCH #{batch_id} → {OUTPUT_PATH}")

    df_transformed \
        .write \
        .mode("append") \
        .partitionBy("market_cap_category") \
        .parquet(OUTPUT_PATH)

    print(f"  ✓ Saved {df_transformed.count()} records")

    print(f"\n  Waiting {TRIGGER_EVERY} for next batch...\n")


# ============================================================
# PART 6 — START STREAMING QUERY
# ============================================================

print("\n✓ Starting streaming query...")

print(f"  Topic      : {KAFKA_TOPIC}")
print(f"  Output     : {OUTPUT_PATH}")
print(f"  Checkpoint : {CHECKPOINT_PATH}")
print(f"  Trigger    : every {TRIGGER_EVERY}")

print("  Press Ctrl+C to stop\n")


query = df_stream \
    .writeStream \
    .foreachBatch(process_batch) \
    .trigger(processingTime=TRIGGER_EVERY) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .start()


try:
    query.awaitTermination()

except KeyboardInterrupt:

    print("\n⏹ Stopped by user")

    query.stop()
    spark.stop()

    print("✓ Spark closed cleanly")
