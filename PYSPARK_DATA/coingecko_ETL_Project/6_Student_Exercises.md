# Student Exercises - CoinGecko ETL Project

## 📝 Exercise 1: Data Exploration with Pandas

### Objective
Explore the CoinGecko dataset using Pandas to understand the data structure and identify patterns.

### Tasks
1. Load the dataset and display the first 10 rows
2. Check the data types of all columns
3. Find the number of missing values in each column
4. Display basic statistics for numeric columns
5. Find the top 5 cryptocurrencies by market cap
6. Find the bottom 5 cryptocurrencies by price
7. Calculate the average price of all cryptocurrencies
8. Count how many cryptocurrencies have a 24h positive price change

### Starter Code
```python
import pandas as pd

# Load data
df = pd.read_csv('input/coingecko_raw_data.csv')

# Your code here
```

### Expected Output
- Shape of the dataset
- Data types summary
- Missing values count
- Statistical summary
- Top/bottom coins lists
- Average price
- Count of positive price changes

---

## 📝 Exercise 2: Data Cleaning with Pandas

### Objective
Clean the dataset by handling missing values, duplicates, and outliers.

### Tasks
1. Identify columns with missing values
2. Fill missing `current_price` with 0
3. Fill missing `market_cap` with 0
4. Remove duplicate rows
5. Remove rows where `current_price` is 0 or negative
6. Convert `last_updated` to datetime format
7. Create a new column `price_usd` that rounds `current_price` to 2 decimal places
8. Save the cleaned data to `cleaned_data_exercise.csv`

### Starter Code
```python
import pandas as pd

df = pd.read_csv('input/coingecko_raw_data.csv')

# Your cleaning code here

# Save cleaned data
df.to_csv('output/cleaned_data_exercise.csv', index=False)
```

### Questions
1. How many rows were removed as duplicates?
2. How many rows had invalid prices?
3. Which columns had the most missing values?

---

## 📝 Exercise 3: Data Transformation with Pandas

### Objective
Create new features and apply business logic to the dataset.

### Tasks
1. Create a column `price_category`:
   - 'High' if price > $1000
   - 'Medium' if price between $10 and $1000
   - 'Low' if price between $1 and $10
   - 'Very Low' if price < $1

2. Create a column `volume_category`:
   - 'High' if 24h volume > $1B
   - 'Medium' if 24h volume between $100M and $1B
   - 'Low' if 24h volume < $100M

3. Create a column `supply_utilization` = `circulating_supply` / `max_supply`
   - Handle division by zero

4. Create a column `price_volatility` = `high_24h` - `low_24h`

5. Filter to show only coins with 'High' price category
6. Show the average `price_volatility` for each `price_category`

### Starter Code
```python
import pandas as pd

df = pd.read_csv('input/coingecko_raw_data.csv')

# Your transformation code here
```

### Challenge
- Create a composite score that combines price, volume, and market cap
- Rank coins by this score

---

## 📝 Exercise 4: Data Aggregation with Pandas

### Objective
Aggregate data to derive insights and summary statistics.

### Tasks
1. Group by `price_category` and calculate:
   - Count of coins
   - Average market cap
   - Average 24h volume
   - Average price change percentage

2. Find the top 3 coins in each `price_category` by market cap

3. Calculate the correlation between:
   - Price and market cap
   - Volume and market cap
   - Price change percentage and volume

4. Create a pivot table showing:
   - Rows: price_category
   - Columns: volume_category
   - Values: count of coins

### Starter Code
```python
import pandas as pd

df = pd.read_csv('input/coingecko_raw_data.csv')

# Your aggregation code here
```

### Questions
1. Which price category has the highest average market cap?
2. Is there a strong correlation between price and market cap?
3. Which combination of price and volume categories has the most coins?

---

## 📝 Exercise 5: PySpark Basics

### Objective
Learn PySpark fundamentals by replicating Pandas operations.

### Tasks
1. Initialize a SparkSession
2. Load the CSV file into a PySpark DataFrame
3. Display the schema of the DataFrame
4. Show the first 5 rows
5. Count the total number of rows
6. Select only the `name`, `symbol`, and `current_price` columns
7. Filter to show only coins with price > $100
8. Order by market cap in descending order
9. Show the top 10 coins by market cap

### Starter Code
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark
spark = SparkSession.builder.appName("Exercise5").getOrCreate()

# Load data
df = spark.read.csv('input/coingecko_raw_data.csv', header=True, inferSchema=True)

# Your PySpark code here

# Stop Spark
spark.stop()
```

### Questions
1. How does PySpark syntax differ from Pandas?
2. What happens when you call `show()` vs `count()`?

---

## 📝 Exercise 6: PySpark Data Cleaning

### Objective
Implement data cleaning operations in PySpark.

### Tasks
1. Handle missing values:
   - Fill `current_price` with 0
   - Fill `market_cap` with 0
   - Fill `total_volume` with 0

2. Remove duplicate rows

3. Filter out rows with `current_price <= 0`

4. Convert string columns to appropriate types if needed

5. Create a new column `price_squared` = `current_price` ** 2

6. Cache the cleaned DataFrame

7. Write the cleaned data to a new CSV file

### Starter Code
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Exercise6").getOrCreate()

df = spark.read.csv('input/coingecko_raw_data.csv', header=True, inferSchema=True)

# Your cleaning code here

spark.stop()
```

### Questions
1. How many rows were removed in each step?
2. Why is caching useful here?
3. How does PySpark handle missing values differently from Pandas?

---

## 📝 Exercise 7: PySpark Transformations

### Objective
Apply PySpark transformations to create new features.

### Tasks
1. Create `price_category` using `when/otherwise`:
   - 'High' if price > $1000
   - 'Medium' if price between $10 and $1000
   - 'Low' if price < $10

2. Create `volume_category`:
   - 'High' if volume > $1B
   - 'Medium' if volume between $100M and $1B
   - 'Low' if volume < $100M

3. Create `supply_ratio` = `circulating_supply` / `max_supply`
   - Use `coalesce` to handle null values

4. Extract year from `ath_date` column

5. Create a flag column `is_stablecoin`:
   - True if price change 24h is between -0.1% and 0.1%
   - False otherwise

### Starter Code
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Exercise7").getOrCreate()

df = spark.read.csv('input/coingecko_raw_data.csv', header=True, inferSchema=True)

# Your transformation code here

spark.stop()
```

### Challenge
- Use window functions to rank coins within each category

---

## 📝 Exercise 8: PySpark Aggregations

### Objective
Perform aggregations and grouping operations in PySpark.

### Tasks
1. Group by `price_category` and calculate:
   - Count
   - Average price
   - Sum of market cap
   - Average 24h volume

2. Find the top 3 coins by market cap in each category

3. Calculate statistics by `ath_year`:
   - Number of coins that reached ATH in each year
   - Average current price for each ATH year

4. Create a summary DataFrame with:
   - Total market cap
   - Average price
   - Total volume
   - Number of coins

### Starter Code
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Exercise8").getOrCreate()

df = spark.read.csv('input/coingecko_raw_data.csv', header=True, inferSchema=True)

# Your aggregation code here

spark.stop()
```

### Questions
1. How does PySpark's groupBy differ from Pandas?
2. What is the difference between `agg()` and individual aggregation functions?

---

## 📝 Exercise 9: Performance Comparison

### Objective
Compare performance between Pandas and PySpark on different data sizes.

### Tasks
1. Create sample datasets of different sizes:
   - Small: 100 rows
   - Medium: 1,000 rows
   - Large: 10,000 rows

2. Time the following operations in both Pandas and PySpark:
   - Loading data
   - Filtering (price > $100)
   - Grouping and aggregation
   - Sorting

3. Record the execution times

4. Create a comparison table

### Starter Code
```python
import pandas as pd
from pyspark.sql import SparkSession
import time

# For timing
def time_operation(operation, description):
    start = time.time()
    result = operation()
    end = time.time()
    print(f"{description}: {end - start:.4f} seconds")
    return result

# Your comparison code here
```

### Questions
1. Which tool is faster for small datasets?
2. At what data size does PySpark become competitive?
3. What overhead does PySpark have?

---

## 📝 Exercise 10: Real-World ETL Pipeline

### Objective
Build a complete ETL pipeline combining all concepts learned.

### Tasks
1. **Extract**: Fetch fresh data from CoinGecko API
2. **Transform**:
   - Clean the data
   - Create derived columns
   - Apply business logic
   - Validate data quality
3. **Load**:
   - Save cleaned data in Parquet format
   - Create aggregated summary tables
   - Generate data quality report

4. **Error Handling**:
   - Handle API failures
   - Log all operations
   - Handle data quality issues

5. **Documentation**:
   - Document the pipeline
   - Create data dictionary
   - Write usage instructions

### Starter Code
```python
import requests
import pandas as pd
from pyspark.sql import SparkSession
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_data():
    """Fetch data from CoinGecko API"""
    pass

def transform_data(df):
    """Clean and transform data"""
    pass

def validate_data(df):
    """Validate data quality"""
    pass

def load_data(df, output_path):
    """Save processed data"""
    pass

def main():
    """Main ETL pipeline"""
    pass

if __name__ == "__main__":
    main()
```

### Deliverables
- Working ETL pipeline code
- Cleaned dataset in Parquet format
- Data quality report
- Documentation

---

## 🎯 Bonus Challenges

### Challenge 1: Advanced Window Functions
Use PySpark window functions to:
- Calculate rolling averages
- Find moving maximums/minimums
- Rank coins within categories
- Calculate percentiles

### Challenge 2: Spark SQL
Convert DataFrame operations to Spark SQL queries:
- Create temporary views
- Write SQL queries
- Compare performance with DataFrame API

### Challenge 3: Machine Learning Integration
- Use PySpark MLlib to:
  - Cluster coins by features
  - Predict price categories
  - Detect anomalies

### Challenge 4: Real-Time Processing
- Set up Spark Structured Streaming
- Process incoming cryptocurrency data
- Calculate real-time metrics

---

## 📊 Project Ideas

### Project 1: Cryptocurrency Portfolio Analyzer
Build a tool that:
- Imports portfolio data
- Enriches with CoinGecko data
- Calculates portfolio metrics
- Generates reports

### Project 2: Market Sentiment Analyzer
Combine price data with:
- Social media sentiment
- News analysis
- Technical indicators

### Project 3: Price Prediction Model
Build a machine learning model to:
- Predict future prices
- Feature engineering from historical data
- Model evaluation and tuning

---

## ✅ Solutions

Solutions for these exercises are available upon request from your instructor. Try to solve them independently first!

---

## 📈 Progress Tracking

Use this checklist to track your progress:

- [ ] Exercise 1: Data Exploration with Pandas
- [ ] Exercise 2: Data Cleaning with Pandas
- [ ] Exercise 3: Data Transformation with Pandas
- [ ] Exercise 4: Data Aggregation with Pandas
- [ ] Exercise 5: PySpark Basics
- [ ] Exercise 6: PySpark Data Cleaning
- [ ] Exercise 7: PySpark Transformations
- [ ] Exercise 8: PySpark Aggregations
- [ ] Exercise 9: Performance Comparison
- [ ] Exercise 10: Real-World ETL Pipeline
- [ ] Bonus Challenge 1: Advanced Window Functions
- [ ] Bonus Challenge 2: Spark SQL
- [ ] Bonus Challenge 3: ML Integration
- [ ] Bonus Challenge 4: Real-Time Processing

---

**Good luck with your exercises! Remember: The best way to learn is by doing.**
