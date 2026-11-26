"""
PySpark ETL Pipeline for CoinGecko Data
This script demonstrates data extraction, transformation, and loading using PySpark
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

print("=" * 60)
print("PYSPARK ETL PIPELINE FOR COINGECKO DATA")
print("=" * 60)

# ============================================
# STEP 0: Initialize Spark Session
# ============================================
print("\n[STEP 0] Initializing Spark Session...")
spark = SparkSession.builder \
    .appName("CoinGeckoETL") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

print("✓ Spark session created")
print(f"Spark version: {spark.version}")

# ============================================
# STEP 1: EXTRACTION (Loading Data)
# ============================================
print("\n[STEP 1] EXTRACTION - Loading data from CSV...")
df = spark.read.csv(
    "input/coingecko_raw_data.csv",
    header=True,
    inferSchema=True
)
print(f"✓ Loaded data")
print(f"✓ Schema: {df.schema}")
print(f"✓ Record count: {df.count()}")

# Cache the DataFrame for multiple operations
df.cache()

# ============================================
# STEP 2: EXPLORATORY DATA ANALYSIS
# ============================================
print("\n[STEP 2] EXPLORATORY DATA ANALYSIS...")
print(f"Data shape: {df.count()} rows, {len(df.columns)} columns")
print(f"\nSchema:")
df.printSchema()

print(f"\nBasic statistics:")
df.describe().show()

print(f"\nMissing values per column:")
df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# ============================================
# STEP 3: DATA CLEANING (Transformation)
# ============================================
print("\n[STEP 3] DATA CLEANING...")

# 3.1 Handle missing values
print("  - Handling missing values...")
df_clean = df.na.fill({
    'current_price': 0,
    'market_cap': 0,
    'total_volume': 0,
    'circulating_supply': 0
})

# Fill max_supply with circulating_supply if missing
df_clean = df_clean.withColumn(
    'max_supply',
    F.coalesce(F.col('max_supply'), F.col('circulating_supply'))
)

# 3.2 Remove duplicates
print("  - Removing duplicates...")
initial_count = df_clean.count()
df_clean = df_clean.dropDuplicates()
final_count = df_clean.count()
print(f"  - Removed {initial_count - final_count} duplicates")

# 3.3 Handle outliers (remove coins with 0 or negative price)
print("  - Handling outliers...")
initial_count = df_clean.count()
df_clean = df_clean.filter(F.col('current_price') > 0)
final_count = df_clean.count()
print(f"  - Removed {initial_count - final_count} records with invalid prices")

print(f"✓ Cleaning complete. Records after cleaning: {df_clean.count()}")

# ============================================
# STEP 4: DATA TRANSFORMATION (Business Logic)
# ============================================
print("\n[STEP 4] DATA TRANSFORMATION...")

# 4.1 Create new columns
print("  - Creating derived columns...")
df_clean = df_clean.withColumn(
    'price_to_market_cap_ratio',
    F.col('current_price') / (F.col('market_cap') + 1)
)
df_clean = df_clean.withColumn(
    'volume_to_market_cap_ratio',
    F.col('total_volume') / (F.col('market_cap') + 1)
)

# 4.2 Categorize coins by market cap using when/otherwise
print("  - Categorizing coins by market cap...")
df_clean = df_clean.withColumn(
    'market_cap_category',
    F.when(F.col('market_cap') >= 100000000000, 'Large Cap')
     .when(F.col('market_cap') >= 10000000000, 'Mid Cap')
     .when(F.col('market_cap') >= 1000000000, 'Small Cap')
     .otherwise('Micro Cap')
)

# 4.3 Calculate price change category
print("  - Categorizing price changes...")
df_clean = df_clean.withColumn(
    'price_change_category',
    F.when(F.col('price_change_percentage_24h') >= 5, 'Strong Positive')
     .when(F.col('price_change_percentage_24h') >= 0, 'Positive')
     .when(F.col('price_change_percentage_24h') >= -5, 'Negative')
     .otherwise('Strong Negative')
)

# 4.4 Extract date components
print("  - Extracting date components...")
df_clean = df_clean.withColumn('ath_date', F.to_timestamp('ath_date'))
df_clean = df_clean.withColumn('ath_year', F.year('ath_date'))
df_clean = df_clean.withColumn('ath_month', F.month('ath_date'))

print("✓ Transformation complete")

# ============================================
# STEP 5: DATA AGGREGATION
# ============================================
print("\n[STEP 5] DATA AGGREGATION...")

# 5.1 Aggregate by market cap category
print("  - Aggregating by market cap category...")
category_stats = df_clean.groupBy('market_cap_category').agg(
    F.mean('current_price').alias('avg_price'),
    F.median('current_price').alias('median_price'),
    F.stddev('current_price').alias('std_price'),
    F.sum('market_cap').alias('total_market_cap'),
    F.count('*').alias('coin_count'),
    F.sum('total_volume').alias('total_volume')
)

print("Category Statistics:")
category_stats.show()

# 5.2 Top 10 coins by market cap
print("\n  - Top 10 coins by market cap...")
top_10 = df_clean.orderBy(F.desc('market_cap')).limit(10)
top_10.select('name', 'symbol', 'current_price', 'market_cap').show()

# ============================================
# STEP 6: DATA VALIDATION
# ============================================
print("\n[STEP 6] DATA VALIDATION...")

# 6.1 Check for negative prices
negative_prices = df_clean.filter(F.col('current_price') < 0).count()
if negative_prices > 0:
    print(f"  ⚠ Warning: Found {negative_prices} records with negative prices")
else:
    print("  ✓ No negative prices found")

# 6.2 Check for zero market cap with non-zero price
zero_mcap = df_clean.filter((F.col('market_cap') == 0) & (F.col('current_price') > 0)).count()
if zero_mcap > 0:
    print(f"  ⚠ Warning: Found {zero_mcap} records with zero market cap but positive price")
else:
    print("  ✓ No inconsistent market cap values found")

# ============================================
# STEP 7: LOADING (Save Cleaned Data)
# ============================================
print("\n[STEP 7] LOADING - Saving cleaned data...")

# Save cleaned data
output_path = "output/coingecko_cleaned_pyspark"
df_clean.write.mode('overwrite').csv(output_path, header=True)
print(f"✓ Cleaned data saved to {output_path}")

# Save aggregated data
agg_output_path = "output/coingecko_aggregated_pyspark"
category_stats.write.mode('overwrite').csv(agg_output_path, header=True)
print(f"✓ Aggregated data saved to {agg_output_path}")

# Save top 10 coins
top10_output_path = "output/top10_coins_pyspark"
top_10.select('name', 'symbol', 'current_price', 'market_cap') \
      .write.mode('overwrite').csv(top10_output_path, header=True)
print(f"✓ Top 10 coins saved to {top10_output_path}")

# ============================================
# STEP 8: PERFORMANCE MONITORING
# ============================================
print("\n[STEP 8] PERFORMANCE MONITORING...")
print(f"Number of partitions: {df_clean.rdd.getNumPartitions()}")

# Show query plan (lazy evaluation demonstration)
print("\nQuery Plan (shows optimized execution):")
df_clean.explain()

# ============================================
# SUMMARY
# ============================================
print("\n" + "=" * 60)
print("PYSPARK ETL PIPELINE SUMMARY")
print("=" * 60)
print(f"Original records: {df.count()}")
print(f"Cleaned records: {df_clean.count()}")
print(f"Records removed: {df.count() - df_clean.count()}")
print(f"Processing mode: Distributed computing")
print(f"Number of partitions: {df_clean.rdd.getNumPartitions()}")
print(f"Output files: 3 CSV files (partitioned)")
print("=" * 60)

# Stop Spark session
spark.stop()
print("✓ Spark session stopped")
