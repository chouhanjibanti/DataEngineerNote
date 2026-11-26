# Quick Start Guide - CoinGecko ETL Project

## 🚀 Get Started in 5 Minutes

This guide will help you set up and run the CoinGecko ETL project quickly.

## Step 1: Install Dependencies

```bash
cd coingecko_ETL_Project
pip install -r requirements.txt
```

## Step 2: Verify Installation

```bash
python -c "import pandas; print('Pandas version:', pandas.__version__)"
python -c "import pyspark; print('PySpark version:', pyspark.__version__)"
```

## Step 3: Fetch Data (Optional)

```bash
python data/fetch_and_save_data.py
```

This will download the latest cryptocurrency data to the `input/` directory.

## Step 4: Run Pandas ETL Pipeline

```bash
python 2_Pandas_ETL_Example.py
```

**Expected Output:**
- Data loading and cleaning steps
- Statistics and aggregations
- 3 output CSV files in `output/` directory

## Step 5: Run PySpark ETL Pipeline

```bash
python 3_PySpark_ETL_Example.py
```

**Expected Output:**
- Spark session initialization
- Distributed processing steps
- Query plan explanation
- 3 output directories in `output/` folder

## Step 6: Compare Results

Open the generated files and compare:
- `output/coingecko_cleaned_pandas.csv` vs `output/coingecko_cleaned_pyspark/`
- Processing time differences
- Output format differences

## 📊 What You'll Learn

### Pandas
- Single-machine data processing
- In-memory operations
- Pythonic syntax
- Quick prototyping

### PySpark
- Distributed computing
- Lazy evaluation
- Cluster processing
- Big data scalability

## 🔧 Troubleshooting

### Issue: Java not found (PySpark)
```bash
# Install Java 8 or higher
# Windows: Download from oracle.com
# Mac: brew install openjdk@8
# Linux: sudo apt install openjdk-8-jdk
```

### Issue: Memory errors (Pandas)
```bash
# Solution: Use PySpark instead for large datasets
python 3_PySpark_ETL_Example.py
```

### Issue: PySpark slow on small data
- This is normal! PySpark has overhead
- For small datasets, Pandas is faster
- PySpark shines with big data (>10GB)

## 📚 Next Steps

1. **Read the comparison**: `1_Pandas_vs_PySpark_Comparison.md`
2. **Study the code**: Review both ETL scripts
3. **Experiment**: Modify the transformation logic
4. **Check the guide**: `4_Teaching_Guide.md` for detailed lessons

## 💡 Tips for Students

- Start with Pandas to understand the logic
- Then move to PySpark to learn distributed computing
- Compare the syntax - they're very similar!
- Focus on understanding WHEN to use each tool

## 🎯 Key Takeaways

1. **Pandas** = Great for small data, fast prototyping
2. **PySpark** = Essential for big data, production systems
3. **Both** have similar syntax, easy to learn both
4. **Choice** depends on data size and use case

---

**Ready to dive deeper? Check out the full README.md and Teaching_Guide.md!**
