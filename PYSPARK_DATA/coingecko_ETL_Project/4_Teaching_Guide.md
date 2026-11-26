# Teaching Guide: CoinGecko ETL Project

## 🎯 Course Structure Overview

This project is designed to teach students the fundamentals of data engineering with a focus on the transition from Pandas to PySpark. Below is a comprehensive guide on what topics to cover and how to structure the lessons.

## 📚 Module 1: Introduction to Data Engineering

### Learning Objectives
- Understand what data engineering is
- Learn the ETL (Extract, Transform, Load) process
- Understand the importance of data quality

### Topics to Cover
1. **What is Data Engineering?**
   - Definition and role in organizations
   - Data pipeline architecture
   - Difference between data engineering and data science

2. **The ETL Process**
   - Extract: Getting data from various sources
   - Transform: Cleaning and processing data
   - Load: Storing data in target systems

3. **Data Quality Concepts**
   - Data completeness
   - Data accuracy
   - Data consistency
   - Data timeliness

### Hands-on Activity
- Review the CoinGecko API and data structure
- Discuss the business value of cryptocurrency data
- Identify potential data quality issues

---

## 📚 Module 2: Pandas Fundamentals for Data Engineering

### Learning Objectives
- Master Pandas for data manipulation
- Understand data cleaning techniques
- Learn data aggregation and transformation

### Topics to Cover

#### 2.1 Data Loading and Exploration
```python
import pandas as pd
df = pd.read_csv('data.csv')
df.head()
df.info()
df.describe()
```

**Teaching Points:**
- Different file formats (CSV, JSON, Parquet)
- Data type inference
- Memory usage considerations

#### 2.2 Data Cleaning
- Handling missing values
- Removing duplicates
- Data type conversion
- Outlier detection and handling

#### 2.3 Data Transformation
- Creating new columns
- Applying functions to columns
- Conditional logic with apply()
- String operations

#### 2.4 Data Aggregation
- GroupBy operations
- Pivot tables
- Reshaping data

### Hands-on Activity
Run `2_Pandas_ETL_Example.py` and discuss each step:
- Line-by-line code review
- Discuss why each cleaning step is necessary
- Analyze the output and results

---

## 📚 Module 3: Introduction to Big Data and Distributed Computing

### Learning Objectives
- Understand the limitations of single-machine processing
- Learn distributed computing concepts
- Understand when to scale beyond Pandas

### Topics to Cover

#### 3.1 The Big Data Problem
- Volume: Data size exceeding RAM
- Velocity: High-speed data generation
- Variety: Different data formats and sources
- Veracity: Data quality and uncertainty

#### 3.2 Distributed Computing Basics
- Cluster computing concepts
- Parallel processing
- Data partitioning
- Network communication overhead

#### 3.3 When to Use Distributed Computing
- Data size thresholds (typically > 10GB)
- Processing time requirements
- Resource availability
- Cost considerations

### Demonstration
- Show memory error with Pandas on large dataset
- Compare processing times on different data sizes
- Visualize the difference between single-node and cluster processing

---

## 📚 Module 4: PySpark Fundamentals

### Learning Objectives
- Understand PySpark architecture
- Learn PySpark DataFrame operations
- Master the difference between transformations and actions

### Topics to Cover

#### 4.1 PySpark Architecture
- Driver program
- Executors
- Cluster manager
- SparkContext/SparkSession

#### 4.2 SparkSession and Data Loading
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()
df = spark.read.csv("data.csv", header=True)
```

**Teaching Points:**
- SparkSession configuration
- Different data sources (CSV, JSON, Parquet, databases)
- Schema inference vs. explicit schema

#### 4.3 DataFrame Operations
- Selecting columns
- Filtering data
- Grouping and aggregation
- Joining DataFrames

#### 4.4 Transformations vs Actions
- **Transformations (Lazy)**
  - select, filter, groupBy, withColumn
  - Build execution plan but don't execute
  
- **Actions (Eager)**
  - show, collect, count, write
  - Trigger actual computation

#### 4.5 Lazy Evaluation
```python
# These don't execute immediately
df_filtered = df.filter(df.price > 100)
df_selected = df_filtered.select('name')

# This triggers execution
df_selected.show()
```

**Teaching Points:**
- Benefits of lazy evaluation
- Query optimization
- Catalyst optimizer

### Hands-on Activity
Run `3_PySpark_ETL_Example.py` and discuss:
- Spark session initialization
- Compare syntax with Pandas
- Explain the query plan with `df.explain()`
- Discuss partitioning

---

## 📚 Module 5: Pandas vs PySpark Comparison

### Learning Objectives
- Understand the trade-offs between Pandas and PySpark
- Learn when to use each tool
- Compare performance characteristics

### Topics to Cover

#### 5.1 Architecture Comparison
| Aspect | Pandas | PySpark |
|--------|--------|---------|
| Processing | Single machine | Distributed cluster |
| Memory | Limited by RAM | Scales across cluster |
| Parallelism | Limited | Built-in |
| Evaluation | Eager | Lazy |

#### 5.2 Performance Comparison
- Small data (< 1GB): Pandas faster
- Medium data (1-10GB): Depends on use case
- Large data (> 10GB): PySpark required
- Very large data (> 100GB): Only PySpark works

#### 5.3 Syntax Comparison
Show side-by-side examples:
- Data loading
- Filtering
- Grouping
- Aggregation
- Joining

#### 5.4 Use Case Scenarios
- **Use Pandas when:**
  - Data fits in memory
  - Quick prototyping
  - Data visualization
  - Complex custom functions
  
- **Use PySpark when:**
  - Data exceeds memory
  - Production ETL pipelines
  - Real-time processing
  - Machine learning on big data

### Hands-on Activity
- Run both ETL pipelines on the same data
- Compare execution times
- Compare memory usage
- Discuss the results

---

## 📚 Module 6: Advanced PySpark Concepts

### Learning Objectives
- Understand PySpark optimization techniques
- Learn about partitioning and caching
- Master Spark SQL

### Topics to Cover

#### 6.1 Partitioning
- What is partitioning?
- Why partitioning matters
- How to choose the right number of partitions
- Repartition vs coalesce

```python
df.repartition(10)  # Increase partitions
df.coalesce(1)      # Decrease partitions
```

#### 6.2 Caching
- When to cache DataFrames
- Cache vs persist
- Memory management

```python
df.cache()  # Cache in memory
df.persist(StorageLevel.DISK_ONLY)  # Persist on disk
```

#### 6.3 Spark SQL
- Running SQL queries on DataFrames
- Creating temporary views
- SQL vs DataFrame API

```python
df.createOrReplaceTempView("coins")
spark.sql("SELECT * FROM coins WHERE price > 100")
```

#### 6.4 Performance Tuning
- Spark configuration options
- Broadcast joins
- Adaptive query execution

### Hands-on Activity
- Experiment with different partition counts
- Measure performance with and without caching
- Compare DataFrame API vs Spark SQL

---

## 📚 Module 7: Data Cleaning and Validation

### Learning Objectives
- Master data cleaning techniques in both Pandas and PySpark
- Learn data validation strategies
- Understand data quality metrics

### Topics to Cover

#### 7.1 Common Data Quality Issues
- Missing values
- Duplicates
- Outliers
- Inconsistent formats
- Invalid data types

#### 7.2 Cleaning Techniques
- **Missing Values**
  - Deletion
  - Imputation (mean, median, mode)
  - Forward/backward fill
  - Domain-specific imputation

- **Duplicates**
  - Exact duplicates
  - Fuzzy matching
  - Business logic deduplication

- **Outliers**
  - Statistical methods (IQR, Z-score)
  - Domain knowledge
  - Visual detection

#### 7.3 Data Validation
- Schema validation
- Business rule validation
- Referential integrity
- Data profiling

### Hands-on Activity
- Identify data quality issues in CoinGecko data
- Implement cleaning strategies
- Create validation rules
- Generate data quality reports

---

## 📚 Module 8: Real-World ETL Pipeline Design

### Learning Objectives
- Design production-ready ETL pipelines
- Understand error handling and logging
- Learn monitoring and alerting

### Topics to Cover

#### 8.1 Pipeline Architecture
- Batch vs streaming
- Lambda architecture
- Pipeline orchestration (Airflow, Prefect)
- Data lineage

#### 8.2 Error Handling
- Try-catch blocks
- Retry logic
- Dead letter queues
- Alerting

#### 8.3 Logging and Monitoring
- Structured logging
- Metrics collection
- Performance monitoring
- Data quality monitoring

#### 8.4 Best Practices
- Idempotency
- Backward compatibility
- Documentation
- Testing

### Hands-on Activity
- Design an ETL pipeline for CoinGecko data
- Add error handling and logging
- Create monitoring dashboards
- Implement data quality checks

---

## 📚 Module 9: Project Implementation

### Learning Objectives
- Apply all learned concepts
- Build a complete ETL solution
- Present findings and recommendations

### Project Requirements
1. **Data Extraction**
   - Fetch data from CoinGecko API
   - Handle API rate limits
   - Implement retry logic

2. **Data Transformation**
   - Clean the data
   - Create derived metrics
   - Implement business logic

3. **Data Loading**
   - Save to multiple formats (CSV, Parquet)
   - Implement partitioning
   - Optimize for query performance

4. **Documentation**
   - Document the pipeline
   - Create data dictionary
   - Write user guide

### Deliverables
- Working ETL pipeline code
- Cleaned dataset
- Data quality report
- Presentation of findings

---

## 🎯 Assessment Strategies

### Quizzes
1. **Module 1-2 Quiz**: Pandas fundamentals
2. **Module 3-4 Quiz**: Big data and PySpark basics
3. **Module 5-6 Quiz**: Advanced concepts and optimization

### Practical Assignments
1. **Assignment 1**: Clean a messy dataset with Pandas
2. **Assignment 2**: Implement the same cleaning in PySpark
3. **Assignment 3**: Optimize a slow PySpark job

### Final Project
- Build a complete ETL pipeline
- Present to class
- Defend design decisions

---

## 💡 Teaching Tips

### For Instructors

1. **Start Simple**: Begin with Pandas, then introduce complexity
2. **Use Real Data**: Cryptocurrency data is engaging and current
3. **Show, Don't Just Tell**: Live coding demonstrations
4. **Encourage Experimentation**: Let students break things and learn
5. **Industry Context**: Explain why companies use these tools

### Common Student Misconceptions

1. **"PySpark is always better"**
   - Clarify: It depends on data size and use case

2. **"More partitions = better performance"**
   - Clarify: Too many partitions cause overhead

3. **"Caching always helps"**
   - Clarify: Caching uses memory, use judiciously

4. **"Big data means millions of rows"**
   - Clarify: It's about data size, not just row count

### Debugging Common Issues

1. **Memory Errors in Pandas**
   - Solution: Use chunking or switch to PySpark

2. **Slow PySpark Jobs**
   - Solution: Check partitioning, skew, and shuffle operations

3. **Java/PySpark Installation Issues**
   - Solution: Use conda or Docker for consistent environments

---

## 📊 Recommended Timeline

### 10-Week Course Structure

| Week | Module | Focus |
|------|--------|-------|
| 1 | Module 1 | Introduction to Data Engineering |
| 2-3 | Module 2 | Pandas Fundamentals |
| 4 | Module 3 | Big Data Concepts |
| 5-6 | Module 4 | PySpark Fundamentals |
| 7 | Module 5 | Pandas vs PySpark Comparison |
| 8 | Module 6 | Advanced PySpark |
| 9 | Module 7-8 | Data Cleaning & Pipeline Design |
| 10 | Module 9 | Final Project Presentations |

### Intensive 3-Day Workshop

| Day | Topics |
|-----|--------|
| Day 1 | Modules 1-2: Pandas and ETL basics |
| Day 2 | Modules 3-5: Big data and PySpark |
| Day 3 | Modules 6-9: Advanced topics and projects |

---

## 🎓 Learning Outcomes

By the end of this course, students will be able to:

1. **Technical Skills**
   - Use Pandas for data manipulation
   - Use PySpark for distributed data processing
   - Design and implement ETL pipelines
   - Clean and validate data

2. **Conceptual Understanding**
   - Understand distributed computing concepts
   - Know when to use Pandas vs PySpark
   - Understand data quality principles
   - Grasp performance optimization techniques

3. **Practical Application**
   - Build production-ready data pipelines
   - Troubleshoot common data engineering issues
   - Make informed technology choices
   - Communicate technical decisions effectively

---

## 📖 Additional Resources

### Books
- "Python for Data Analysis" by Wes McKinney
- "Learning Spark" by O'Reilly
- "Designing Data-Intensive Applications" by Martin Kleppmann

### Online Courses
- Coursera: "Data Engineering" specializations
- Udemy: "PySpark for Data Engineering"
- Databricks Academy: Spark certification courses

### Practice Datasets
- Kaggle datasets
- UCI Machine Learning Repository
- Google Public Data Explorer

---

## 🔄 Continuous Improvement

### Feedback Mechanisms
- Student surveys after each module
- Assignment difficulty ratings
- Project peer reviews
- Industry feedback on curriculum

### Curriculum Updates
- Update data sources regularly
- Incorporate new PySpark features
- Add emerging tools and technologies
- Align with industry trends

---

**This teaching guide provides a comprehensive framework for teaching data engineering using the CoinGecko ETL project. Adjust the pace and depth based on your students' background and learning objectives.**
