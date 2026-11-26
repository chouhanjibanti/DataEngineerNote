"""
Pandas ETL Pipeline for CoinGecko Data
This script demonstrates data extraction, transformation, and loading using Pandas
"""

import pandas as pd
import numpy as np
from datetime import datetime

print("=" * 60)
print("PANDAS ETL PIPELINE FOR COINGECKO DATA")
print("=" * 60)

# ============================================
# STEP 1: EXTRACTION (Loading Data)
# ============================================
print("\n[STEP 1] EXTRACTION - Loading data from CSV...")
df = pd.read_csv("input/coingecko_raw_data.csv")
print(f"✓ Loaded {len(df)} records")
print(f"✓ Columns: {list(df.columns)}")

# ============================================
# STEP 2: EXPLORATORY DATA ANALYSIS
# ============================================
print("\n[STEP 2] EXPLORATORY DATA ANALYSIS...")
print(f"Data shape: {df.shape}")
print(f"Data types:\n{df.dtypes}")
print(f"\nBasic statistics:\n{df.describe()}")
print(f"\nMissing values:\n{df.isnull().sum()}")

# ============================================
# STEP 3: DATA CLEANING (Transformation)
# ============================================
print("\n[STEP 3] DATA CLEANING...")

# 3.1 Handle missing values
print("  - Handling missing values...")
df_clean = df.copy()

# Fill numeric columns with 0 where appropriate
numeric_cols = ['current_price', 'market_cap', 'total_volume', 'circulating_supply']
for col in numeric_cols:
    if col in df_clean.columns:
        df_clean[col] = df_clean[col].fillna(0)

# Fill max_supply with circulating_supply if missing
if 'max_supply' in df_clean.columns and 'circulating_supply' in df_clean.columns:
    df_clean['max_supply'] = df_clean['max_supply'].fillna(df_clean['circulating_supply'])

# 3.2 Remove duplicates
print("  - Removing duplicates...")
initial_count = len(df_clean)
df_clean = df_clean.drop_duplicates()
print(f"  - Removed {initial_count - len(df_clean)} duplicates")

# 3.3 Convert data types
print("  - Converting data types...")
if 'last_updated' in df_clean.columns:
    df_clean['last_updated'] = pd.to_datetime(df_clean['last_updated'])

# 3.4 Handle outliers (example: remove coins with 0 price)
print("  - Handling outliers...")
df_clean = df_clean[df_clean['current_price'] > 0]
print(f"  - Removed {len(df) - len(df_clean)} records with invalid prices")

print(f"✓ Cleaning complete. Records after cleaning: {len(df_clean)}")

# ============================================
# STEP 4: DATA TRANSFORMATION (Business Logic)
# ============================================
print("\n[STEP 4] DATA TRANSFORMATION...")

# 4.1 Create new columns
print("  - Creating derived columns...")
df_clean['price_to_market_cap_ratio'] = df_clean['current_price'] / (df_clean['market_cap'] + 1)
df_clean['volume_to_market_cap_ratio'] = df_clean['total_volume'] / (df_clean['market_cap'] + 1)

# 4.2 Categorize coins by market cap
print("  - Categorizing coins by market cap...")
def categorize_market_cap(market_cap):
    if market_cap >= 100000000000:  # > $100B
        return 'Large Cap'
    elif market_cap >= 10000000000:  # > $10B
        return 'Mid Cap'
    elif market_cap >= 1000000000:  # > $1B
        return 'Small Cap'
    else:
        return 'Micro Cap'

df_clean['market_cap_category'] = df_clean['market_cap'].apply(categorize_market_cap)

# 4.3 Calculate price change category
print("  - Categorizing price changes...")
def categorize_price_change(change_pct):
    if change_pct >= 5:
        return 'Strong Positive'
    elif change_pct >= 0:
        return 'Positive'
    elif change_pct >= -5:
        return 'Negative'
    else:
        return 'Strong Negative'

df_clean['price_change_category'] = df_clean['price_change_percentage_24h'].apply(categorize_price_change)

# 4.4 Extract date components
print("  - Extracting date components...")
if 'ath_date' in df_clean.columns:
    df_clean['ath_year'] = pd.to_datetime(df_clean['ath_date']).dt.year
    df_clean['ath_month'] = pd.to_datetime(df_clean['ath_date']).dt.month

print(f"✓ Transformation complete. New columns: {['price_to_market_cap_ratio', 'volume_to_market_cap_ratio', 'market_cap_category', 'price_change_category']}")

# ============================================
# STEP 5: DATA AGGREGATION
# ============================================
print("\n[STEP 5] DATA AGGREGATION...")

# 5.1 Aggregate by market cap category
print("  - Aggregating by market cap category...")
category_stats = df_clean.groupby('market_cap_category').agg({
    'current_price': ['mean', 'median', 'std'],
    'market_cap': ['sum', 'count'],
    'total_volume': 'sum'
}).round(2)
print(category_stats)

# 5.2 Top 10 coins by market cap
print("\n  - Top 10 coins by market cap...")
top_10 = df_clean.nlargest(10, 'market_cap')[['name', 'symbol', 'current_price', 'market_cap']]
print(top_10.to_string(index=False))

# ============================================
# STEP 6: DATA VALIDATION
# ============================================
print("\n[STEP 6] DATA VALIDATION...")

# 6.1 Check for negative prices
negative_prices = df_clean[df_clean['current_price'] < 0]
if len(negative_prices) > 0:
    print(f"  ⚠ Warning: Found {len(negative_prices)} records with negative prices")
else:
    print("  ✓ No negative prices found")

# 6.2 Check for zero market cap with non-zero price
zero_mcap = df_clean[(df_clean['market_cap'] == 0) & (df_clean['current_price'] > 0)]
if len(zero_mcap) > 0:
    print(f"  ⚠ Warning: Found {len(zero_mcap)} records with zero market cap but positive price")
else:
    print("  ✓ No inconsistent market cap values found")

# ============================================
# STEP 7: LOADING (Save Cleaned Data)
# ============================================
print("\n[STEP 7] LOADING - Saving cleaned data...")

# Save cleaned data
output_path = "output/coingecko_cleaned_pandas.csv"
df_clean.to_csv(output_path, index=False)
print(f"✓ Cleaned data saved to {output_path}")

# Save aggregated data
agg_output_path = "output/coingecko_aggregated_pandas.csv"
category_stats.to_csv(agg_output_path)
print(f"✓ Aggregated data saved to {agg_output_path}")

# Save top 10 coins
top10_output_path = "output/top10_coins_pandas.csv"
top_10.to_csv(top10_output_path, index=False)
print(f"✓ Top 10 coins saved to {top10_output_path}")

# ============================================
# SUMMARY
# ============================================
print("\n" + "=" * 60)
print("PANDAS ETL PIPELINE SUMMARY")
print("=" * 60)
print(f"Original records: {len(df)}")
print(f"Cleaned records: {len(df_clean)}")
print(f"Records removed: {len(df) - len(df_clean)}")
print(f"Processing time: Single machine, in-memory")
print(f"Output files: 3 CSV files generated")
print("=" * 60)
