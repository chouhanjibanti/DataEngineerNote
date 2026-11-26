# Employee Data Analytics Pipeline

A hands-on Data Engineering course built around **one evolving project**: cleaning,
transforming, storing, and analyzing employee data — first with Python/Pandas, then
with Hadoop (concepts), and finally with PySpark.

Every phase uses the same dataset: [`employee_data.csv`](./employee_data.csv)

```csv
EmployeeID,Name,Department,City,Age,Salary,Experience
101,Rahul,IT,Indore,25,45000,2
102,Aman,HR,Bhopal,28,55000,5
103,Jay,IT,Indore,24,42000,2
104,Banti,Sales,Ujjain,30,65000,7
105,Rohit,IT,Indore,26,50000,3
```

---

## 📂 Folder Structure

```
DataEngineeringCourse/
│
├── Day01_Python/
├── Day02_CSV/
├── Day03_DataCleaning/
├── Day04_Transformation/
├── Day05_PandasAnalysis/
│
├── Day06_HDFS/
├── Day07_Hadoop/
│
├── Day08_SparkSession/
├── Day09_DataFrames/
├── Day10_Transformations/
├── Day11_SQL/
├── Day12_Joins/
├── Day13_WindowFunctions/
├── Day14_Project/
│
├── Datasets/
│   └── employee_data.csv
│
├── etl_pipeline.py          # Phase 1 - Pandas ETL
├── pyspark_etl_pipeline.py  # Phase 3 - PySpark ETL
└── README.md
```

---

## 🗺️ Course Roadmap

### Phase 1 — Python for Data Engineering (Days 1–5)
| Day | Topic | Key Skills |
|---|---|---|
| 1 | Introduction | Data lifecycle, ETL vs ELT, batch vs streaming, variables, lists, dicts |
| 2 | Working with CSV | `pd.read_csv()`, `head()`, `tail()`, `shape`, `info()` |
| 3 | Data Cleaning | `isnull()`, `fillna()`, `dropna()` |
| 4 | Data Transformation | new/renamed/dropped columns, sort, filter |
| 5 | Data Analysis | `groupby()`, aggregates, first report |

### Phase 2 — Hadoop (Days 6–7, concepts only)
| Day | Topic |
|---|---|
| 6 | Why Hadoop, HDFS, distributed storage (simulated block split) |
| 7 | Docker + `hdfs dfs` commands (`-mkdir`, `-put`, `-cat`, `-ls`) |

### Phase 3 — Spark (Days 8–14)
| Day | Topic |
|---|---|
| 8 | Install PySpark, create `SparkSession`, read CSV |
| 9 | `select()`, `filter()`, `where()`, `orderBy()`, `withColumn()` |
| 10 | `groupBy()`, `count()`, `avg()`, `max()`, `min()`, `sum()` |
| 11 | Spark SQL — `createOrReplaceTempView()` + `spark.sql()` |
| 12 | Joins across `employees.csv` + `department.csv` |
| 13 | Window functions — `rank()`, `dense_rank()`, `row_number()` |
| 14 | Mini project — Employee Dashboard |

### Phase 4 — End-to-End Pipeline (Day 15)
```
CSV → Python Cleaning → HDFS → PySpark → Analysis → Output CSV → Dashboard
```

---

## 🧑‍🏫 Teaching Pattern (per lecture)

| Segment | Time | What Happens |
|---|---|---|
| Theory | 10% | Real-life analogy (e.g. "What happens after you order on Swiggy?") |
| Whiteboard | 10% | Draw the architecture (`CSV → HDFS → Spark → Dashboard`) |
| Python Demo | 30% | Live code in VS Code |
| Student Activity | 20% | Students modify the demo code |
| Assignment | 20% | 5 practice questions |
| Interview Prep | 10% | Concept questions (e.g. `filter()` vs `where()`) |

---

## 🔧 Tech Focus (2026 industry priorities)

Per current hiring trends, Hadoop/MapReduce gets minimal time; the bulk goes to
Spark and the modern data stack:

| Skill | Priority |
|---|---|
| PySpark | ⭐⭐⭐⭐⭐ Must learn |
| Databricks | ⭐⭐⭐⭐⭐ Must learn |
| Kafka | ⭐⭐⭐⭐⭐ Must learn |
| Airflow | ⭐⭐⭐⭐⭐ Must learn |
| Snowflake | ⭐⭐⭐⭐ High demand |
| AWS Glue | ⭐⭐⭐⭐ High demand |
| HDFS / Hadoop concepts | ✅ Understand only, don't deep-dive |

---

## ▶️ Running the Practical Pipelines

### 1. Pandas ETL (Phase 1)
```bash
pip install pandas
python etl_pipeline.py
```
Produces `output/employee_report.csv` and prints a department-wise summary.

### 2. PySpark ETL (Phase 3)
```bash
pip install pyspark
python pyspark_etl_pipeline.py
```
Runs the same pipeline using Spark DataFrames + Spark SQL, and writes
`output/employee_report_spark/` (Parquet + CSV).

Both scripts follow the same **Extract → Transform → Load** structure so students
can directly compare the Pandas and PySpark approaches side by side.


=============================================================

