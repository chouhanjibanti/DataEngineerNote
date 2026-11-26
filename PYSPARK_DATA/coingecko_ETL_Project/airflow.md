# Apache Airflow — ETL Pipeline Orchestration
# CoinGecko Project — Simple Guide for Students

---

## What is Apache Airflow?

Apache Airflow is a **workflow orchestration tool** — it schedules, monitors, and manages your ETL pipelines automatically.

Think of it as the **manager/supervisor** of your ETL pipeline:

```
Without Airflow:   You manually run scripts → forget to run → data is stale → no alerts
With Airflow:      Airflow runs scripts automatically → retries on failure → sends alerts
```

---

## Simple Analogy

```
Airflow  =  Alarm Clock + Task Manager + Error Reporter

Alarm Clock   → runs your ETL every 15 min / hourly / daily
Task Manager  → runs tasks in the right ORDER (extract first, then transform, then load)
Error Reporter → if something fails, it retries and sends an alert
```

---

## Airflow vs Kafka — What's the Difference?

Students often confuse these two. They solve different problems:

| | Apache Kafka | Apache Airflow |
|---|---|---|
| **What it does** | Streams data in real time | Schedules and orchestrates tasks |
| **Analogy** | Conveyor belt (continuous) | Alarm clock + task manager (scheduled) |
| **Runs** | Continuously (every second) | On schedule (every 15 min / hourly) |
| **Used for** | Live data streaming | Batch ETL pipelines |
| **In this project** | Live CoinGecko prices | Schedule the batch ETL script |

**Together:** Airflow can schedule the Kafka producer to start/stop, or schedule the batch PySpark ETL to run every hour.

---

## 4 Core Concepts in Airflow

### 1. DAG (Directed Acyclic Graph)
A DAG is your **pipeline definition** — it tells Airflow:
- What tasks to run
- In what order
- On what schedule

```
DAG = "coingecko_etl_pipeline"
  │
  ├── Task 1: extract_data    (call CoinGecko API)
  ├── Task 2: transform_data  (run PySpark ETL)
  └── Task 3: validate_output (check records saved)

Order: Task1 → Task2 → Task3
Schedule: every 1 hour
```

### 2. Task
A Task is a **single unit of work** inside a DAG.

```python
# Example tasks in your CoinGecko ETL:
Task 1 → fetch data from CoinGecko API
Task 2 → run 3_PySpark_ETL_Example.py
Task 3 → check output folder has new files
Task 4 → send success/failure notification
```

### 3. Schedule
How often Airflow triggers the DAG automatically.

```
"@hourly"           → every hour
"@daily"            → every day at midnight
"*/15 * * * *"      → every 15 minutes   (cron syntax)
"0 */6 * * *"       → every 6 hours
```

### 4. Operator
An Operator defines **what kind of task** to run.

| Operator | What it does |
|---|---|
| `PythonOperator` | runs a Python function |
| `BashOperator` | runs a shell command |
| `HttpSensor` | waits until an API is available |
| `EmailOperator` | sends email on success/failure |

---

## Your 4 Requirements — How Airflow Handles Each

### 1. Schedule — Run every 15 minutes or hourly
```python
dag = DAG(
    dag_id="coingecko_etl",
    schedule_interval="*/15 * * * *",   # every 15 minutes
    # OR
    schedule_interval="@hourly",         # every hour
)
```

### 2. Trigger — Run API extraction or batch processing script
```python
# Runs your existing 3_PySpark_ETL_Example.py automatically
run_pyspark = BashOperator(
    task_id="run_pyspark_etl",
    bash_command="python /opt/airflow/scripts/3_PySpark_ETL_Example.py",
)
```

### 3. Monitor and Retry — Retry on failure, check logs
```python
# Airflow automatically retries failed tasks
default_args = {
    "retries"          : 3,              # try 3 times before marking as failed
    "retry_delay"      : timedelta(minutes=5),  # wait 5 min between retries
    "email_on_failure" : True,           # send email if all retries fail
}
```

Logs are visible in the **Airflow Web UI** at `http://localhost:8080`

### 4. Validate Output — Verify processed records
```python
# A Python task that checks output files exist
def validate_output():
    import os
    output = "output/coingecko_cleaned_pyspark"
    files = os.listdir(output)
    assert len(files) > 0, "No output files found — ETL may have failed"
    print(f"✓ Validation passed — {len(files)} output files found")

validate = PythonOperator(
    task_id="validate_output",
    python_callable=validate_output,
)
```

---

## Architecture — Airflow + Your ETL Project

```
┌──────────────────────────────────────────────────────────┐
│                    AIRFLOW SCHEDULER                      │
│                                                          │
│  Every 15 min / hourly:                                  │
│                                                          │
│  DAG: coingecko_etl_pipeline                             │
│       │                                                  │
│       ▼                                                  │
│  [Task 1] fetch_data                                     │
│       → calls CoinGecko API                              │
│       → saves raw JSON to input/                         │
│       │                                                  │
│       ▼  (only runs if Task 1 succeeded)                 │
│  [Task 2] run_pyspark_etl                                │
│       → runs 3_PySpark_ETL_Example.py                    │
│       → output saved to output/                          │
│       │                                                  │
│       ▼  (only runs if Task 2 succeeded)                 │
│  [Task 3] validate_output                                │
│       → checks output files exist                        │
│       → verifies record count > 0                        │
│       │                                                  │
│       ▼                                                  │
│  [SUCCESS] → log green in Web UI                         │
│  [FAILURE] → retry up to 3 times → email alert           │
└──────────────────────────────────────────────────────────┘
```

---

## Setup — Using Docker (Same Approach as Kafka)

### Folder Structure to Create
```
coingecko_ETL_Project/
└── airflow/
    ├── docker-compose.yml      ← starts Airflow via Docker
    ├── dags/
    │   └── coingecko_dag.py    ← your pipeline definition
    └── scripts/
        └── fetch_coingecko.py  ← API fetch script called by Airflow
```

---

### Step 1 — docker-compose.yml (Start Airflow)

```yaml
version: '3.8'

# ── Airflow needs a database to store DAG state ───────────
x-airflow-common: &airflow-common
  image: apache/airflow:2.7.0
  environment:
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CORE__LOAD_EXAMPLES: "false"   # hide example DAGs
  volumes:
    - ./dags:/opt/airflow/dags              # your DAG files
    - ./scripts:/opt/airflow/scripts        # your Python scripts
  depends_on:
    - postgres

services:

  # ── PostgreSQL (Airflow metadata store) ─────────────────
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow

  # ── Airflow Web UI (http://localhost:8080) ───────────────
  airflow-webserver:
    <<: *airflow-common
    ports:
      - "8080:8080"
    command: webserver

  # ── Airflow Scheduler (triggers DAGs on schedule) ────────
  airflow-scheduler:
    <<: *airflow-common
    command: scheduler

  # ── One-time setup (creates admin user) ──────────────────
  airflow-init:
    <<: *airflow-common
    command: >
      bash -c "airflow db init &&
               airflow users create
               --username admin
               --password admin
               --firstname Admin
               --lastname User
               --role Admin
               --email admin@example.com"
```

Run with:
```bash
cd airflow
docker-compose up -d
```

Web UI → http://localhost:8080  (login: admin / admin)

---

### Step 2 — fetch_coingecko.py (API Script)

```python
# airflow/scripts/fetch_coingecko.py
# Called by Airflow Task 1 — fetches live data and saves to CSV

import requests
import csv
import os
from datetime import datetime

API_URL = "https://api.coingecko.com/api/v3/coins/markets"
OUTPUT  = "/opt/airflow/scripts/input/coingecko_raw_data.csv"

def fetch_and_save():
    params = {"vs_currency": "usd", "order": "market_cap_desc", "per_page": 100}
    response = requests.get(API_URL, params=params, timeout=15)
    coins = response.json()

    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)

    with open(OUTPUT, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=coins[0].keys())
        writer.writeheader()
        writer.writerows(coins)

    print(f"✓ Saved {len(coins)} coins to {OUTPUT}")

if __name__ == "__main__":
    fetch_and_save()
```

---

### Step 3 — coingecko_dag.py (The DAG — Main File)

```python
# airflow/dags/coingecko_dag.py
# This is the pipeline definition Airflow reads automatically

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

# ── Default settings applied to ALL tasks ─────────────────
default_args = {
    "owner"            : "data_team",
    "retries"          : 3,                      # retry 3 times on failure
    "retry_delay"      : timedelta(minutes=5),   # wait 5 min between retries
    "email_on_failure" : False,                  # set True + add email for alerts
}

# ── DAG definition ─────────────────────────────────────────
dag = DAG(
    dag_id="coingecko_etl_pipeline",
    description="Fetch CoinGecko data, run PySpark ETL, validate output",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="*/15 * * * *",    # run every 15 minutes
    # schedule_interval="@hourly",       # OR run every hour
    catchup=False,                       # don't backfill missed runs
)

# ── TASK 1: Fetch fresh data from CoinGecko API ───────────
task_fetch = BashOperator(
    task_id="fetch_coingecko_data",
    bash_command="python /opt/airflow/scripts/fetch_coingecko.py",
    dag=dag,
)

# ── TASK 2: Run PySpark ETL on the fetched data ───────────
task_pyspark = BashOperator(
    task_id="run_pyspark_etl",
    bash_command="python /opt/airflow/scripts/3_PySpark_ETL_Example.py",
    dag=dag,
)

# ── TASK 3: Validate output files exist ───────────────────
def validate_output():
    output_path = "/opt/airflow/scripts/output/coingecko_cleaned_pyspark"
    if not os.path.exists(output_path):
        raise FileNotFoundError(f"Output folder missing: {output_path}")
    files = [f for f in os.listdir(output_path) if f.endswith(".csv")]
    if len(files) == 0:
        raise ValueError("No CSV output files found — ETL may have failed")
    print(f"✓ Validation passed — {len(files)} output files found")

task_validate = PythonOperator(
    task_id="validate_output",
    python_callable=validate_output,
    dag=dag,
)

# ── TASK ORDER (dependency chain) ─────────────────────────
# Task 1 runs first → if success, Task 2 runs → if success, Task 3 runs
task_fetch >> task_pyspark >> task_validate
#          ↑↑
#    ">>" means "then run"
```

---

### Step 4 — How to Run

```bash
# Terminal 1 — Start Airflow
cd airflow
docker-compose up -d

# Wait 30 seconds for Airflow to initialize

# Open Web UI
# http://localhost:8080
# Username: admin
# Password: admin

# Enable your DAG in the UI (toggle the ON/OFF switch)
# Airflow will now run it automatically on schedule
```

---

## Airflow Web UI — What You See

```
http://localhost:8080

DAGs page:
┌──────────────────────────┬────────┬──────────────┬─────────┐
│ DAG Name                 │ Active │ Schedule     │ Last Run│
├──────────────────────────┼────────┼──────────────┼─────────┤
│ coingecko_etl_pipeline   │  ON ●  │ */15 * * * * │ Success │
└──────────────────────────┴────────┴──────────────┴─────────┘

Click DAG → Grid View:
  Batch 1:  [fetch ✓] → [pyspark ✓] → [validate ✓]   GREEN
  Batch 2:  [fetch ✓] → [pyspark ✗] → [validate -]   RED (retrying...)
  Batch 3:  [fetch ✓] → [pyspark ✓] → [validate ✓]   GREEN

Click any task → View Logs → see exact Python output
```

---

## Monitor and Retry — How Airflow Handles Failures

```
Task fails
    │
    ▼
Airflow waits 5 minutes (retry_delay)
    │
    ▼
Retry attempt 1
    │
    ├── Success → continue to next task
    └── Fail    → wait 5 min → retry attempt 2
                      │
                      ├── Success → continue
                      └── Fail    → retry attempt 3
                                      │
                                      ├── Success → continue
                                      └── Fail    → mark task as FAILED
                                                      → send email alert
                                                      → stop DAG run
```

Logs for every retry are stored and viewable in the Web UI — click any red task → Logs.

---

## Summary — Airflow in Your ETL Project

| Step | What Airflow Does |
|---|---|
| **Schedule** | Triggers DAG every 15 min or hourly automatically |
| **Extract** | Runs `fetch_coingecko.py` → saves fresh CSV |
| **Transform** | Runs `3_PySpark_ETL_Example.py` → cleans and transforms |
| **Validate** | Checks output files exist and are non-empty |
| **Retry** | Retries failed tasks up to 3 times with 5-min delay |
| **Monitor** | Web UI at localhost:8080 shows green/red per task |
| **Logs** | Every task run stores full logs — click to view |

---

## Airflow + Kafka Together in One Project

When both are used:

```
┌─────────────────────────────────────────────────┐
│              AIRFLOW (Orchestrator)              │
│                                                  │
│  Schedule: every 1 hour                          │
│                                                  │
│  Task 1 → Start Kafka Producer (bash command)    │
│  Task 2 → Run PySpark Kafka Consumer (30 min)    │
│  Task 3 → Stop Kafka Producer                    │
│  Task 4 → Validate Parquet output                │
└─────────────────────────────────────────────────┘
         │
         ▼
┌──────────────────┐      ┌──────────────────────┐
│  Kafka Producer  │─────►│  Kafka Topic          │
│  (runs 30 min)   │      │  "crypto-prices"      │
└──────────────────┘      └──────────┬───────────┘
                                     │
                                     ▼
                          ┌──────────────────────┐
                          │  PySpark Consumer     │
                          │  (processes stream)   │
                          └──────────────────────┘
```

**Airflow** decides WHEN things run.
**Kafka** handles HOW data flows between systems.
They are complementary — not competing.

---

## Further Reading

- [Apache Airflow Official Docs](https://airflow.apache.org/docs/)
- [Airflow Docker Setup Guide](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
- [Cron Expression Generator](https://crontab.guru/) — for writing schedule_interval values
