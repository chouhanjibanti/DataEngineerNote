"""
Employee Data Analytics Pipeline — Phase 1 (Pandas)
=====================================================
A simple, teachable ETL pipeline for learning data engineering:

    EXTRACT  -> read raw CSV data from file system
    TRANSFORM -> clean data, add derived columns, perform aggregations
    LOAD      -> write processed data to CSV + generate analytics report

This pipeline demonstrates the core ETL process used in data engineering.
Each phase represents a fundamental concept that students must understand.

Run:
    python etl_pipeline.py

Learning Objectives:
- Understand the ETL (Extract, Transform, Load) process
- Learn Pandas DataFrame operations for data manipulation
- Practice data cleaning techniques (handling missing values)
- Implement business logic through derived columns
- Generate basic analytics reports
"""

# Import required libraries
# os: For file system operations (creating directories, path handling)
# pandas: For data manipulation and analysis (the primary tool for this ETL)
import os
import pandas as pd

# Configuration constants - makes the code maintainable and easy to modify
# RAW_FILE: Path to the input data source (raw employee data)
RAW_FILE = "input/employee_data.csv"
# OUTPUT_DIR: Directory where processed data will be saved
OUTPUT_DIR = "output"
# OUTPUT_FILE: Full path to the output file (combines directory and filename)
# os.path.join() is platform-independent (works on Windows, Mac, Linux)
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "employee_report.csv")


# ---------------------------------------------------------------------------
# PHASE 1: EXTRACT
# ---------------------------------------------------------------------------
# The Extract phase is about getting data from various sources into our system
# Sources can be: CSV files, databases, APIs, JSON files, etc.
# In this example, we're extracting from a local CSV file

# def → Used to define a function.
# extract → Function name; in ETL, it usually means reading/getting data from a source.
# path → Parameter that receives the file path or data source location.
# path: str → Type hint indicating that path is expected to be a string.
# pd.DataFrame → Type hint indicating that the function is expected to return a Pandas DataFrame.

def extract(path: str) -> pd.DataFrame:
    """
    Read the raw CSV file into a Pandas DataFrame.
    
    Args:
        path (str): File path to the CSV file
    
    Returns:
        pd.DataFrame: Loaded data in tabular format
    
    Teaching Points:
    - pd.read_csv() is the most common way to load CSV data
    - DataFrame is Pandas' main data structure (like an Excel sheet)
    - Always validate that data loaded correctly (check row/column count)
    """
    print(f"[EXTRACT] Reading {path} ...")
    # pd.read_csv() loads the CSV file into memory as a DataFrame
    # This is the starting point for our ETL pipeline
    df = pd.read_csv(path)
    # df.shape returns (rows, columns) - useful for validation
    print(f"[EXTRACT] Loaded {df.shape[0]} rows, {df.shape[1]} columns")
    return df


# ---------------------------------------------------------------------------
# PHASE 2: TRANSFORM
# ---------------------------------------------------------------------------
# The Transform phase is where we clean, enrich, and structure the data
# This is often the most complex phase in real-world ETL pipelines
# We break it into smaller functions for better organization and testing

# def → Used to define a function.
# clean → Function name; in ETL, it usually means cleaning or transforming the data.
# df → Parameter that receives a Pandas DataFrame.
# df: pd.DataFrame → Type hint indicating that df is expected to be a Pandas DataFrame.
# pd.DataFrame → Type hint indicating that the function is expected to return a Pandas DataFrame.

def clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handle missing values - real-world data is rarely perfect.
    
    Args:
        df (pd.DataFrame): Raw data with potential missing values
    
    Returns:
        pd.DataFrame: Cleaned data with missing values handled
    
    Teaching Points:
    - Missing data is common in real datasets
    - Different strategies for different data types (categorical vs numeric)
    - fillna() is the primary method for handling missing values
    - Always validate the cleaning operation worked correctly
    """
    print("[TRANSFORM] Cleaning data ...")

    # Count total missing values before cleaning (for validation)
    # isnull() returns True for missing values, sum() counts them
# before → Variable hai jo missing/null values ki count store karega.

# df.isnull() → DataFrame mein har cell ko check karta hai ki value NULL/NaN hai ya nahi.
# .sum() → Har column mein kitni null values hain, unka count karta hai.
# .sum() → Sabhi columns ke null counts ko add karke total null values deta hai.
# before = df.isnull().sum().sum() → Cleaning se pehle DataFrame mein total kitni missing values hain, woh before mein store karta hai.

    before = df.isnull().sum().sum()

    # Strategy 1: For categorical data (text), fill with 'Unknown'
    # This preserves the record while marking it as incomplete

# for col in ["Department", "City", "Name"]: → Loop karke Department, City, aur Name columns ko ek-ek karke process karta hai.
# if col in df.columns: → Check karta hai ki current column DataFrame df mein exist karta hai ya nahi.
# df[col] → Current column ko select karta hai, jaise df["Department"].
# .fillna("Unknown") → Current column ki missing (NaN) values ko "Unknown" se replace karta hai.
# df[col] = ... → Cleaned column ko wapas DataFrame mein store/update karta hai.
# Overall → Department, City, aur Name mein jahan bhi missing value hogi, wahan "Unknown" fill ho jayega.

    for col in ["Department", "City", "Name"]:
        if col in df.columns:
            # fillna() replaces missing values with the specified value
            df[col] = df[col].fillna("Unknown")

    # Strategy 2: For numeric data, fill with the column mean
    # This maintains statistical properties of the dataset
# for col in ["Age", "Salary", "Experience"]: → Loop karke Age, Salary, aur Experience columns ko ek-ek karke process karta hai.
# if col in df.columns: → Check karta hai ki current column DataFrame df mein exist karta hai ya nahi.
# df[col] → Current column ko select karta hai, jaise df["Age"].
# df[col].mean() → Current column ki average value calculate karta hai.
# df[col].fillna(df[col].mean()) → Missing (NaN) values ko column ki average value se replace karta hai.
# df[col] = ... → Updated column ko DataFrame mein wapas store karta hai.
# Overall → Age, Salary, aur Experience mein missing values ko unke respective mean/average se fill karta hai.
    for col in ["Age", "Salary", "Experience"]:
        if col in df.columns:
            # mean() calculates the average value of the column
            df[col] = df[col].fillna(df[col].mean())

    # Count missing values after cleaning to verify the operation
    after = df.isnull().sum().sum()
    print(f"[TRANSFORM] Missing values fixed: {before} -> {after}")
    return df

# def - Defines a function
# enrich - Name of the function
# df - Input parameter that contains the DataFrame
# pd.DataFrame - Specifies that df should be a Pandas DataFrame
# -> pd.DataFrame - Specifies that the function will return a Pandas DataFrame

def enrich(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add derived columns - create new data from existing data.
    
    Args:
        df (pd.DataFrame): Cleaned data
    
    Returns:
        pd.DataFrame: Data with additional derived columns
    
    Teaching Points:
    - Derived columns add business value to raw data
    - Column arithmetic works element-wise in Pandas
    - apply() with lambda functions for custom logic
    - Conditional logic for categorization
    """
    print("[TRANSFORM] Adding derived columns ...")
    
    # Derived Column 1: Bonus (10% of salary)
    # This is a simple arithmetic operation on a column
    # Pandas applies the operation to every row automatically
# df["Bonus"] → DataFrame mein naya column "Bonus" create karta hai.
# df["Salary"] → Salary column ki values ko select karta hai.
# * 0.10 → Salary ko 0.10 se multiply karta hai (10% calculate karta hai).
# df["Salary"] * 0.10 → Har employee ki salary ka 10% calculate karta hai.
# df["Bonus"] = ... → Calculated bonus value ko Bonus column mein store karta hai.
# Overall → Har employee ke liye unki salary ka 10% bonus calculate karke naya column banata hai.
    df["Bonus"] = df["Salary"] * 0.10
    
    # Derived Column 2: Total Pay (Salary + Bonus)
    # Combines multiple columns into a meaningful metric
# df["TotalPay"] → DataFrame mein naya column "TotalPay" create karta hai.
# df["Salary"] → Salary column ki values ko select karta hai.
# df["Bonus"] → Bonus column ki values ko select karta hai.
# df["Salary"] + df["Bonus"] → Salary aur Bonus ko add karta hai.
# df["TotalPay"] = ... → Total calculated value ko TotalPay column mein store karta hai.
# Overall → Har employee ke liye Salary + Bonus ka total calculate karke naya column banata hai.
    df["TotalPay"] = df["Salary"] + df["Bonus"]
    
    # Derived Column 3: Experience Level (categorization)
    # Uses apply() with a lambda function for custom logic
    # Categorizes employees based on years of experience
    # This demonstrates conditional logic in data transformation
# df["ExperienceLevel"] → DataFrame mein naya column "ExperienceLevel" create karta hai.
# df["Experience"] → Experience column ki values ko select karta hai.
# .apply() → Har value par custom function apply karta hai.
# lambda x: → Anonymous function define karta hai jo har experience value x ko process karta hai.
# x >= 5 → Check karta hai ki experience 5 years ya usse zyada hai.
# "Senior" → Agar experience >= 5 hai, to "Senior" assign karta hai.
# else ("Mid" if x >= 3 else "Junior") → Agar 5 nahi hai, to 3 check karta hai.
# x >= 3 → Check karta hai ki experience 3 years ya usse zyada hai.
# "Mid" → Agar experience >= 3 hai, to "Mid" assign karta hai.
# else "Junior" → Agar experience 3 se kam hai, to "Junior" assign karta hai.
# Overall → Employees ko experience ke base par categorize karta hai: Senior (5+), Mid (3-4), Junior (0-2).
    df["ExperienceLevel"] = df["Experience"].apply(
        lambda x: "Senior" if x >= 5 else ("Mid" if x >= 3 else "Junior")
    )
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Orchestrate all transformation steps in sequence.
    
    Args:
        df (pd.DataFrame): Raw data from extract phase
    
    Returns:
        pd.DataFrame: Fully transformed data ready for loading
    
    Teaching Points:
    - Breaking transformations into smaller, testable functions
    - Sequential processing (clean before enrich)
    - Function composition pattern in data pipelines
    """
    # Step 1: Clean the data (handle missing values)
    df = clean(df)
    # Step 2: Enrich the data (add derived columns)
    df = enrich(df)
    return df


# ---------------------------------------------------------------------------
# PHASE 3: LOAD
# ---------------------------------------------------------------------------
# The Load phase is about storing processed data for downstream use
# Destinations can be: CSV files, databases, data warehouses, APIs, etc.
# In this example, we're loading to a local CSV file

# def - Defines a function
# load - Name of the function
# df - Input parameter containing the DataFrame
# pd.DataFrame - Specifies that df is a Pandas DataFrame
# output_path - Parameter containing the path where output will be saved
# str - Specifies that output_path should be a string
# -> None - Specifies that the function does not return any value
def load(df: pd.DataFrame, output_path: str) -> None:
    """
    Write the processed data to disk for downstream consumption.
    
    Args:
        df (pd.DataFrame): Transformed data ready for storage
        output_path (str): File path where data will be saved
    
    Returns:
        None (function performs side effect of writing file)
    
    Teaching Points:
    - Always create output directories if they don't exist
    - to_csv() is the standard way to write CSV files
    - index=False prevents writing row numbers (usually not needed)
    - This makes the processed data available to other systems
    """
    # Create the output directory if it doesn't exist
    # exist_ok=True prevents error if directory already exists

    # os - Provides functions for working with files and directories
    # makedirs - Creates the required directory/folder
    # os.path.dirname(output_path) - Gets the directory path from output_path
    # exist_ok=True - Does not give an error if the directory already exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Write the DataFrame to CSV file
    # index=False means don't save the row index (0, 1, 2, ...)
    # df - The Pandas DataFrame that we want to save
    # to_csv() - Converts the DataFrame into a CSV file
    # output_path - Specifies the location and filename where the CSV will be saved
    # index=False - Prevents Pandas from writing the DataFrame index as an extra column
    df.to_csv(output_path, index=False)
    print(f"[LOAD] Report written to {output_path}")


# ---------------------------------------------------------------------------
# ANALYSIS / REPORT (Bonus Phase - what dashboards would consume)
# ---------------------------------------------------------------------------
# This phase generates insights from the processed data
# In production, this might feed into BI tools like Power BI, Tableau, etc.
def report(df: pd.DataFrame) -> None:
    """
    Generate analytics report from processed data.
    
    Args:
        df (pd.DataFrame): Transformed data ready for analysis
    
    Returns:
        None (prints report to console)
    
    Teaching Points:
    - Basic aggregations: count, max, min, mean
    - Grouping data for categorical analysis
    - Combining multiple metrics in a single report
    - This demonstrates the business value of ETL pipelines
    """
    print("\n===== EMPLOYEE ANALYTICS REPORT =====")
    
    # Basic count of total employees
    print(f"Total Employees      : {len(df)}")
    
    # Find highest paid employee and their name
    # max() finds the highest value, idxmax() finds the row index
# print() - Displays the result on the screen
# f-string - Allows us to insert values directly inside the text
# df['Salary'] - Selects the Salary column from the DataFrame
# .max() - Finds the highest salary
# df.loc[] - Selects a specific row and column from the DataFrame
# df['Salary'].idxmax() - Finds the index/position of the employee with the highest salary
# 'Name' - Selects the Name column of that employee
# Result - Prints the highest salary along with the employee's name
    print(f"Highest Salary       : {df['Salary'].max()} ({df.loc[df['Salary'].idxmax(), 'Name']})")


# print() - Displays the result on the screen
# f-string - Allows us to insert values directly inside the text
# df['Salary'] - Selects the Salary column from the DataFrame
# .min() - Finds the lowest salary
# df.loc[] - Selects a specific row and column from the DataFrame
# df['Salary'].idxmin() - Finds the index/position of the employee with the lowest salary
# 'Name' - Selects the Name of that employee
# Result - Prints the lowest salary along with the employee's name
    # Find lowest paid employee and their name
    print(f"Lowest Salary        : {df['Salary'].min()} ({df.loc[df['Salary'].idxmin(), 'Name']})")


    # Calculate average experience across all employees
    # :.2f formats to 2 decimal places
# print() - Displays the result on the screen
# f-string - Allows us to insert values directly inside the text
# df['Experience'] - Selects the Experience column from the DataFrame
# .mean() - Calculates the average experience
# :.2f - Formats the result to exactly 2 decimal places
# "years" - Displays the unit of the calculated experience
    print(f"Average Experience   : {df['Experience'].mean():.2f} years")

    
    # Group by Department and calculate average salary per department
    # groupby() is essential for categorical analysis
# df.groupby("Department") - Groups the data based on each Department
# ["Salary"] - Selects the Salary column from each department
# .mean() - Calculates the average salary for each department
# .round(2) - Rounds the average salary to 2 decimal places
# print() - Displays the department-wise average salary
    print("\nDepartment-wise Average Salary:")
    print(df.groupby("Department")["Salary"].mean().round(2))


    # Group by City and count employees per city
# print() - Displays the result on the screen
# df.groupby("City") - Groups the data based on each City
# ["EmployeeID"] - Selects the EmployeeID column
# .count() - Counts the number of employees in each city
    print("\nCity-wise Employee Count:")
    print(df.groupby("City")["EmployeeID"].count())
    print("======================================\n")


# ---------------------------------------------------------------------------
# PIPELINE ENTRYPOINT
# ---------------------------------------------------------------------------
# This function orchestrates the entire ETL pipeline
# It calls each phase in sequence: Extract -> Transform -> Load -> Report
def run_pipeline():
    """
    Execute the complete ETL pipeline in sequence.
    
    Teaching Points:
    - Pipeline orchestration: calling functions in the right order
    - Data flows through each phase sequentially
    - Each phase receives data from the previous phase
    - This pattern is used in production data pipelines
    """
    # Phase 1: Extract raw data from source
    df = extract(RAW_FILE)
    
    # Phase 2: Transform the data (clean + enrich)
    df = transform(df)
    
    # Phase 3: Load processed data to destination
    load(df, OUTPUT_FILE)
    
    # Bonus Phase: Generate analytics report
    report(df)


# This is the standard Python entry point pattern
# if __name__ == "__main__" ensures the code only runs when the file is executed directly
# It prevents the code from running when the file is imported as a module
if __name__ == "__main__":
    run_pipeline()
