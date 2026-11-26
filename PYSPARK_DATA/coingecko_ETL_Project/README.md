# CoinGecko ETL Project - Pandas vs PySpark Comparison

## 📚 Project Overview
This project demonstrates the differences between Pandas and PySpark for data engineering tasks using real cryptocurrency data from CoinGecko API. It's designed for students to understand when to use each tool and why PySpark is preferred for big data processing.

## 🎯 Learning Objectives
By the end of this project, students will understand:
- The fundamental differences between Pandas and PySpark
- When to use Pandas vs PySpark
- How to implement ETL pipelines in both frameworks
- Key PySpark concepts (lazy evaluation, transformations, actions)
- Real-world data cleaning and transformation techniques

## 📁 Project Structure
```
coingecko_ETL_Project/
├── input/
│   └── coingecko_raw_data.csv          # Raw cryptocurrency data (input)
├── output/
│   ├── coingecko_cleaned_pandas.csv   # Cleaned data from Pandas
│   ├── coingecko_aggregated_pandas.csv # Aggregated data from Pandas
│   ├── top10_coins_pandas.csv          # Top 10 coins from Pandas
│   ├── coingecko_cleaned_pyspark/     # Cleaned data from PySpark (partitioned)
│   ├── coingecko_aggregated_pyspark/   # Aggregated data from PySpark (partitioned)
│   └── top10_coins_pyspark/            # Top 10 coins from PySpark (partitioned)
├── data/
│   └── fetch_and_save_data.py          # Script to fetch fresh data from API
├── 1_Pandas_vs_PySpark_Comparison.md   # Detailed comparison document
├── 2_Pandas_ETL_Example.py             # Pandas ETL implementation
├── 3_PySpark_ETL_Example.py            # PySpark ETL implementation
├── 4_Teaching_Guide.md                 # Comprehensive teaching guide
├── 5_Quick_Start_Guide.md              # Quick start for students
├── 6_Student_Exercises.md              # Hands-on exercises
├── requirements.txt                    # Python dependencies
└── README.md                          # This file
```

## 🚀 Getting Started

### Prerequisites
- Python 3.8 or higher
- Java 8 or higher (required for PySpark)
- Basic understanding of Python

### Installation

1. **Install dependencies:**
```bash
pip install -r requirements.txt
```

2. **Verify PySpark installation:**
```bash
pyspark --version
```

## 📊 Data Source
- **API**: CoinGecko Market Data API
- **Endpoint**: `https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd`
- **Data includes**: Cryptocurrency prices, market caps, volume, supply, and historical data

### Key Data Fields
- `id`, `symbol`, `name` - Coin identification
- `current_price` - Current price in USD
- `market_cap` - Total market capitalization
- `total_volume` - 24h trading volume
- `circulating_supply`, `total_supply`, `max_supply` - Supply metrics
- `ath`, `atl` - All-time high/low prices
- `price_change_percentage_24h` - 24h price change

## 🔄 Running the ETL Pipelines

### Option 1: Pandas ETL Pipeline
```bash
cd coingecko_ETL_Project
python 2_Pandas_ETL_Example.py
```

**What it does:**
- Loads CSV data into memory
- Performs data cleaning (missing values, duplicates, outliers)
- Creates derived columns (ratios, categories)
- Aggregates data by market cap categories
- Saves cleaned and aggregated data

### Option 2: PySpark ETL Pipeline
```bash
cd coingecko_ETL_Project
python 3_PySpark_ETL_Example.py
```

**What it does:**
- Initializes Spark session
- Loads CSV data in distributed manner
- Performs same transformations as Pandas
- Demonstrates lazy evaluation and query optimization
- Saves output as partitioned CSV files

### Option 3: Fetch Fresh Data
```bash
cd coingecko_ETL_Project
python data/fetch_and_save_data.py
```

This will fetch the latest cryptocurrency data from CoinGecko API and save it to the `input/` directory.

## 📈 Key Differences Demonstrated

### 1. Memory Usage
- **Pandas**: Loads entire dataset into RAM
- **PySpark**: Processes data in partitions across cluster

### 2. Processing Model
- **Pandas**: Eager evaluation (immediate execution)
- **PySpark**: Lazy evaluation (builds execution plan)

### 3. Syntax Comparison
```python
# Pandas
df[df['price'] > 100]
df.groupby('category').mean()

# PySpark
df.filter(df.price > 100)
df.groupBy('category').mean()
```

### 4. Performance Characteristics
- **Small data (< 1GB)**: Pandas is faster
- **Large data (> 10GB)**: PySpark scales better
- **Very large data (> 100GB)**: Only PySpark works

## 🎓 Teaching Points for Students

### 1. Understanding Data Scale
- Start with Pandas for small datasets
- Recognize when data exceeds memory limits
- Understand distributed computing concepts

### 2. ETL Pipeline Components
- **Extract**: Loading data from various sources
- **Transform**: Cleaning, validating, enriching data
- **Load**: Saving processed data to target systems

### 3. Data Quality Checks
- Missing value handling
- Duplicate detection
- Outlier identification
- Data type validation

### 4. Business Logic Implementation
- Creating derived metrics
- Categorization logic
- Aggregation strategies

### 5. Performance Optimization
- Partitioning in PySpark
- Caching strategies
- Query plan optimization

## 🔍 Advanced PySpark Concepts Covered

### Lazy Evaluation
```python
# These don't execute immediately
df_filtered = df.filter(df.price > 100)
df_selected = df_filtered.select('name', 'price')

# Only this triggers execution
df_selected.show()
```

### Transformations vs Actions
- **Transformations**: filter, select, groupBy, withColumn (lazy)
- **Actions**: show, collect, count, write (eager)

### Catalyst Optimizer
PySpark automatically optimizes query plans:
```python
df.explain()  # Shows the optimized execution plan
```

## 📊 Output Files

### Pandas Output
- `data/coingecko_cleaned_pandas.csv` - Cleaned dataset
- `data/coingecko_aggregated_pandas.csv` - Aggregated statistics
- `data/top10_coins_pandas.csv` - Top 10 coins by market cap

### PySpark Output
- `data/coingecko_cleaned_pyspark/` - Partitioned cleaned data
- `data/coingecko_aggregated_pyspark/` - Partitioned aggregated data
- `data/top10_coins_pyspark/` - Partitioned top 10 coins

## 🛠️ Troubleshooting

### PySpark Issues
**Problem**: Java not found
```bash
# Set JAVA_HOME environment variable
export JAVA_HOME=/path/to/java
```

**Problem**: Memory errors
```bash
# Increase Spark driver memory
spark-submit --driver-memory 4g 3_PySpark_ETL_Example.py
```

### Pandas Issues
**Problem**: Memory errors with large datasets
- Solution: Use chunking or switch to PySpark
```python
chunk_size = 10000
for chunk in pd.read_csv('large_file.csv', chunksize=chunk_size):
    process(chunk)
```

## 📚 Additional Resources

### Documentation
- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [CoinGecko API Documentation](https://www.coingecko.com/en/api)

### Recommended Reading
- "Learning Spark" by O'Reilly
- "Python for Data Analysis" by Wes McKinney
- "Designing Data-Intensive Applications" by Martin Kleppmann

## 🎯 Next Steps for Students

1. **Experiment**: Modify the transformation logic
2. **Scale Up**: Test with larger datasets
3. **Optimize**: Try different partitioning strategies
4. **Integrate**: Connect to databases or other data sources
5. **Visualize**: Create visualizations from the cleaned data

## 💡 Project Extensions

- Add real-time streaming with Spark Structured Streaming
- Implement machine learning predictions
- Connect to a database for storage
- Build a dashboard with the processed data
- Add data quality monitoring and alerting

## 📝 Notes for Instructors

### Key Teaching Points
1. **Start simple**: Begin with Pandas, then introduce PySpark
2. **Use real data**: Cryptocurrency data is engaging and constantly changing
3. **Show both approaches**: Demonstrate the same task in both frameworks
4. **Discuss trade-offs**: When to use each tool
5. **Industry relevance**: Emphasize that PySpark is used in production

### Common Student Questions
- **Q**: Why not just use Pandas for everything?
- **A**: Pandas can't handle data larger than RAM. PySpark scales horizontally.

- **Q**: Is PySpark always slower?
- **A**: For small data, yes (overhead). For big data, it's the only option.

- **Q**: Can I use Pandas syntax in PySpark?
- **A**: Similar but not identical. PySpark has its own API.

## 📄 License
This project is for educational purposes. Cryptocurrency data is provided by CoinGecko API.

## 👥 Contributing
This is a teaching project. Feel free to modify and adapt for your curriculum.

---

**Happy Learning! 🚀**
