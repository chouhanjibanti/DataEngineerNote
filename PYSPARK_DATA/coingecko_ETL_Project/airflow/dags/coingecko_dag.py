"""
============================================================
Apache Airflow DAG — CoinGecko ETL Pipeline
============================================================
DAG ID   : coingecko_etl_pipeline
Schedule : Every 15 minutes  (change to @hourly if preferred)

PIPELINE (3 tasks in order):

  Task 1: fetch_coingecko_data
      → runs scripts/fetch_coingecko.py
      → calls CoinGecko API, saves fresh CSV

  Task 2: run_pyspark_etl
      → runs scripts/3_PySpark_ETL_Example.py
      → cleans, transforms, aggregates data
      → saves output CSV files

  Task 3: validate_output
      → runs scripts/validate_output.py
      → checks output files exist + record count > 0

  Task 1 >> Task 2 >> Task 3
  (each task only runs if the previous one succeeded)

RETRY:   3 retries with 5-minute delay
MONITOR: http://localhost:8081  (admin / admin)
============================================================
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os


# ============================================================
# PART 1 — DEFAULT ARGS
# These settings apply to ALL tasks in the DAG
# ============================================================

default_args = {
    "owner"             : "data_team",

    # ── Retry config (Requirement 3: Monitor and Retry) ───
    "retries"           : 3,                       # retry 3 times on failure
    "retry_delay"       : timedelta(minutes=5),    # wait 5 min between retries
    "retry_exponential_backoff": False,

    # ── Alert config ──────────────────────────────────────
    "email_on_failure"  : False,    # set True and add email= to get alerts
    "email_on_retry"    : False,

    # ── Timeout — kill task if it runs too long ───────────
    "execution_timeout" : timedelta(minutes=30),
}


# ============================================================
# PART 2 — DAG DEFINITION
# (Requirement 1: Schedule every 15 min or hourly)
# ============================================================

dag = DAG(
    dag_id="coingecko_etl_pipeline",
    description="Fetch CoinGecko API → PySpark ETL → Validate output",
    default_args=default_args,

    # ── Schedule ──────────────────────────────────────────
    schedule_interval="*/15 * * * *",   # every 15 minutes
    # schedule_interval="@hourly",      # ← uncomment for hourly

    start_date=datetime(2026, 1, 1),
    catchup=False,       # don't run missed schedules from the past
    tags=["coingecko", "etl", "pyspark"],
)

# Path where scripts are mounted inside the Docker container
SCRIPTS = "/opt/airflow/scripts"


# ============================================================
# PART 3 — TASK 1: FETCH DATA
# (Requirement 2: Trigger API extraction)
# BashOperator runs a shell command
# ============================================================

task_fetch = BashOperator(
    task_id="fetch_coingecko_data",
    bash_command=f"python {SCRIPTS}/fetch_coingecko.py",
    dag=dag,
)


# ============================================================
# PART 4 — TASK 2: RUN PYSPARK ETL
# (Requirement 2: Trigger batch processing script)
# Runs your existing 3_PySpark_ETL_Example.py unchanged
# ============================================================

task_pyspark = BashOperator(
    task_id="run_pyspark_etl",
    bash_command=f"cd {SCRIPTS} && python 3_PySpark_ETL_Example.py",
    dag=dag,
)


# ============================================================
# PART 5 — TASK 3: VALIDATE OUTPUT
# (Requirement 4: Validate processed records and output)
# PythonOperator runs a Python function directly
# ============================================================

def run_validation():
    """
    Inline validation — called by PythonOperator.
    Checks output folder and record counts.
    Raises exception if validation fails → Airflow marks task FAILED.
    """
    import os
    import csv

    output_dir = os.path.join(SCRIPTS, "output", "coingecko_cleaned_pyspark")

    # Check 1: output folder exists
    if not os.path.exists(output_dir):
        raise FileNotFoundError(
            f"Output folder not found: {output_dir}\n"
            "PySpark ETL may have failed or not run yet."
        )

    # Check 2: at least one CSV file
    csv_files = [f for f in os.listdir(output_dir) if f.endswith(".csv")]
    if not csv_files:
        raise ValueError(
            f"No CSV output files in: {output_dir}\n"
            "ETL ran but produced no output."
        )

    # Check 3: record count
    first_csv = os.path.join(output_dir, csv_files[0])
    with open(first_csv, "r") as f:
        record_count = sum(1 for _ in f) - 1    # subtract header

    if record_count < 10:
        raise ValueError(
            f"Too few records: {record_count} (expected >= 10)\n"
            "Data quality issue — check fetch script."
        )

    # All checks passed
    print(f"✓ Validation PASSED")
    print(f"  Output folder : {output_dir}")
    print(f"  CSV files     : {len(csv_files)}")
    print(f"  Records found : {record_count}")


task_validate = PythonOperator(
    task_id="validate_output",
    python_callable=run_validation,
    dag=dag,
)


# ============================================================
# PART 6 — TASK ORDER (DEPENDENCY CHAIN)
# >> means "run next only if previous succeeded"
#
# fetch → pyspark → validate
# ============================================================

task_fetch >> task_pyspark >> task_validate

#   Task 1             Task 2             Task 3
#  [fetch_data]  →→  [run_pyspark]  →→  [validate]
#   if FAIL: retry 3x  if FAIL: retry 3x  if FAIL: retry 3x
#   logs in UI          logs in UI          logs in UI
