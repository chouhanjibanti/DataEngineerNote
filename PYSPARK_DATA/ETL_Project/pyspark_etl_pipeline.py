"""
Employee Data Analytics Pipeline — Phase 3 (PySpark)
=======================================================
Same ETL shape as etl_pipeline.py, rebuilt with Spark DataFrames + Spark SQL,
so students can compare Pandas vs PySpark line by line.

    EXTRACT   -> spark.read.csv() (distributed data loading)
    TRANSFORM -> clean, add columns, window function (distributed processing)
    LOAD      -> write Parquet + CSV, run Spark SQL report (distributed storage)

This demonstrates the same ETL concepts but using distributed computing.
Key differences from Pandas version:
- Uses SparkSession instead of pandas
- Lazy evaluation (transformations don't execute immediately)
- Distributed processing across multiple machines
- Spark SQL for analytics instead of pandas aggregations
- Window functions for advanced analytics

Run:
    pip install pyspark
    python pyspark_etl_pipeline.py

Learning Objectives:
- Understand PySpark vs Pandas differences
- Learn distributed data processing concepts
- Practice Spark SQL for analytics
- Understand window functions in Spark
- Compare single-machine vs distributed computing
"""

# Import required PySpark libraries
# os: For file system operations (same as Pandas version)
# SparkSession: Main entry point for PySpark (like pandas but for distributed computing)
# functions as F: Collection of built-in Spark functions (like pandas operations)
# Window: For window functions (advanced analytics not easily done in pandas)
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Configuration constants (same as Pandas version)
RAW_FILE = "input/employee_data.csv"
OUTPUT_DIR = "output/employee_report_spark"


def get_spark():
    """
    Create and configure a Spark session.
    
    Returns:
        SparkSession: Configured Spark session for distributed processing
    
    Teaching Points:
    - SparkSession is the entry point for all Spark functionality
    - appName() gives the job a name (visible in Spark UI)
    - master("local[*]") runs Spark locally using all available CPU cores
    - In production, this would connect to a Spark cluster
    - getOrCreate() returns existing session or creates new one
    """
    return (
        SparkSession.builder
        .appName("EmployeeAnalyticsPipeline")  # Job name for monitoring
        .master("local[*]")  # Use all local CPU cores (for learning)
        .getOrCreate()  # Create or get existing Spark session
    )


# ---------------------------------------------------------------------------
# PHASE 1: EXTRACT (PySpark version)
# ---------------------------------------------------------------------------
def extract(spark, path: str):
    """
    Read the raw CSV file into a Spark DataFrame.
    
    Args:
        spark: SparkSession object
        path (str): File path to the CSV file
    
    Returns:
        Spark DataFrame: Loaded data in distributed format
    
    Teaching Points:
    - spark.read.csv() is PySpark's equivalent of pd.read_csv()
    - header=True means first row contains column names
    - inferSchema=True automatically detects data types
    - Spark DataFrames are distributed (unlike pandas DataFrames)
    - count() is an action (triggers execution), unlike pandas len()
    """
    print(f"[EXTRACT] Reading {path} ...")
    # spark.read.csv() loads data in distributed manner
    # header=True: first row contains column names
    # inferSchema=True: automatically detect data types
    df = spark.read.csv(path, header=True, inferSchema=True)
    # df.count() is an ACTION - triggers actual data loading
    # len(df.columns) is metadata operation - doesn't trigger execution
    print(f"[EXTRACT] Loaded {df.count()} rows, {len(df.columns)} columns")
    return df


# ---------------------------------------------------------------------------
# PHASE 2: TRANSFORM (PySpark version)
# ---------------------------------------------------------------------------
def clean(df):
    """
    Handle missing values using PySpark operations.
    
    Args:
        df: Spark DataFrame with potential missing values
    
    Returns:
        Spark DataFrame: Cleaned data
    
    Teaching Points:
    - fillna() works similarly to pandas but with dictionary syntax
    - F.mean() is a Spark aggregation function
    - select() chooses columns (like pandas df[])
    - first()[0] extracts the scalar value from the result
    - These are TRANSFORMATIONS (lazy evaluation)
    """
    print("[TRANSFORM] Cleaning data ...")
    
    # Fill categorical columns with 'Unknown'
    # Dictionary syntax: {column_name: fill_value}
    df = df.fillna({"Department": "Unknown", "City": "Unknown", "Name": "Unknown"})
    
    # Fill numeric columns with mean (more complex in Spark)
    for col_name in ["Age", "Salary", "Experience"]:
        # Calculate mean for the column
        # F.mean() is a Spark aggregate function
        # F.col() references a column
        mean_val = df.select(F.mean(F.col(col_name))).first()[0]
        # Fill missing values with calculated mean
        df = df.fillna({col_name: mean_val})
    return df


def enrich(df):
    """
    Add derived columns using PySpark operations including window functions.
    
    Args:
        df: Spark DataFrame (cleaned data)
    
    Returns:
        Spark DataFrame: Data with additional derived columns
    
    Teaching Points:
    - withColumn() adds new columns (like pandas df[] = )
    - F.col() references columns (like pandas df['col'])
    - F.when() is Spark's conditional logic (like pandas apply with lambda)
    - Window functions enable advanced analytics (ranking per group)
    - Window functions are powerful but complex - unique to Spark/SQL
    """
    print("[TRANSFORM] Adding derived columns + window function ...")
    
    # Derived Column 1: Bonus (10% of salary)
    # withColumn() adds/renames columns
    # F.col() references the Salary column
    df = df.withColumn("Bonus", F.col("Salary") * 0.10)
    
    # Derived Column 2: Total Pay (Salary + Bonus)
    df = df.withColumn("TotalPay", F.col("Salary") + F.col("Bonus"))
    
    # Derived Column 3: Experience Level (categorization)
    # F.when().when().otherwise() is Spark's conditional logic
    # Similar to pandas: df.apply(lambda x: "Senior" if x >= 5 else ...)
    df = df.withColumn(
        "ExperienceLevel",
        F.when(F.col("Experience") >= 5, "Senior")
         .when(F.col("Experience") >= 3, "Mid")
         .otherwise("Junior"),
    )

    # Window Function: Rank employees by salary within each department
    # This is an advanced feature not easily done in pandas
    # Window functions operate on a "window" of related rows
    dept_window = Window.partitionBy("Department").orderBy(F.col("Salary").desc())
    # partitionBy("Department"): Group by department
    # orderBy(F.col("Salary").desc()): Order by salary (highest first)
    # F.rank().over(): Apply ranking function to the window
    df = df.withColumn("SalaryRankInDept", F.rank().over(dept_window))
    return df


def transform(df):
    """
    Orchestrate all transformation steps in sequence.
    
    Args:
        df: Spark DataFrame from extract phase
    
    Returns:
        Spark DataFrame: Fully transformed data
    
    Teaching Points:
    - Same pattern as Pandas version: sequential function calls
    - All operations are TRANSFORMATIONS (lazy evaluation)
    - No actual computation happens until an ACTION is called
    """
    df = clean(df)
    df = enrich(df)
    return df


# ---------------------------------------------------------------------------
# PHASE 3: LOAD (PySpark version)
# ---------------------------------------------------------------------------
def load(df, output_dir: str):
    """
    Write the processed data to disk in multiple formats.
    
    Args:
        df: Spark DataFrame ready for storage
        output_dir (str): Directory where data will be saved
    
    Returns:
        None (side effect: writes files)
    
    Teaching Points:
    - Spark can write to multiple formats (Parquet, CSV, JSON, etc.)
    - Parquet is Spark's preferred format (columnar, compressed)
    - write.mode("overwrite") overwrites existing data
    - These are ACTIONS (trigger actual computation)
    - Distributed writing (data written by multiple executors)
    """
    os.makedirs(output_dir, exist_ok=True)
    parquet_path = os.path.join(output_dir, "parquet")
    csv_path = os.path.join(output_dir, "csv")
    
    # Write to Parquet format (Spark's preferred format)
    # Parquet is columnar storage, compressed, efficient for analytics
    # mode("overwrite"): Replace existing data
    df.write.mode("overwrite").parquet(parquet_path)
    
    # Write to CSV format (for compatibility with other tools)
    # option("header", True): Include column names in first row
    df.write.mode("overwrite").option("header", True).csv(csv_path)
    print(f"[LOAD] Report written to {parquet_path} and {csv_path}")


# ---------------------------------------------------------------------------
# ANALYSIS / REPORT (Spark SQL version)
# ---------------------------------------------------------------------------
def report(spark, df):
    """
    Generate analytics report using Spark SQL.
    
    Args:
        spark: SparkSession object
        df: Spark DataFrame ready for analysis
    
    Returns:
        None (prints report to console)
    
    Teaching Points:
    - Spark SQL allows using SQL syntax on DataFrames
    - createOrReplaceTempView() registers DataFrame as a SQL table
    - spark.sql() executes SQL queries
    - .show() displays results (ACTION)
    - This is powerful for people with SQL background
    """
    # Register DataFrame as a temporary SQL view
    # This allows us to use SQL queries on the DataFrame
    df.createOrReplaceTempView("employee")

    print("\n===== EMPLOYEE ANALYTICS REPORT (Spark SQL) =====")

    # Query 1: Highest paid employee per department
    # Uses the SalaryRankInDept column we created with window function
    print("-> Highest paid employee per department:")
    spark.sql(
        """
        SELECT Department, Name, Salary
        FROM employee
        WHERE SalaryRankInDept = 1  -- Rank 1 = highest paid in department
        ORDER BY Department
        """
    ).show()  # .show() is an ACTION - triggers query execution

    # Query 2: Department-wise average salary
    # Demonstrates GROUP BY and aggregation in SQL
    print("-> Department-wise average salary:")
    spark.sql(
        """
        SELECT Department, ROUND(AVG(Salary), 2) AS AvgSalary
        FROM employee
        GROUP BY Department
        ORDER BY AvgSalary DESC
        """
    ).show()

    # Query 3: City-wise employee count
    # Demonstrates COUNT aggregation
    print("-> City-wise employee count:")
    spark.sql(
        """
        SELECT City, COUNT(*) AS EmployeeCount
        FROM employee
        GROUP BY City
        ORDER BY EmployeeCount DESC
        """
    ).show()

    print("===================================================\n")


# ---------------------------------------------------------------------------
# PIPELINE ENTRYPOINT
# ---------------------------------------------------------------------------
def run_pipeline():
    """
    Execute the complete PySpark ETL pipeline.
    
    Teaching Points:
    - Same orchestration pattern as Pandas version
    - Additional step: spark.stop() to clean up resources
    - Spark sessions consume resources, must be stopped
    """
    # Initialize Spark session
    spark = get_spark()
    
    # Execute ETL phases
    df = extract(spark, RAW_FILE)
    df = transform(df)
    load(df, OUTPUT_DIR)
    report(spark, df)
    
    # Clean up: Stop Spark session to free resources
    # Important in production to avoid resource leaks
    spark.stop()


# Standard Python entry point pattern
if __name__ == "__main__":
    run_pipeline()
