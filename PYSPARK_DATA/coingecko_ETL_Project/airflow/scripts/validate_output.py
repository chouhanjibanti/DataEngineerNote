"""
============================================================
Task 3 Script — Validate ETL Output
============================================================
Called by Airflow DAG:  coingecko_etl_pipeline
Task name            :  validate_output

WHAT IT DOES:
  - Checks output folder exists
  - Verifies CSV files were created
  - Reads one file and checks record count > 0
  - Exits with code 1 if validation fails (Airflow marks task FAILED)
  - Exits with code 0 if all checks pass (Airflow marks task SUCCESS)

RUN MANUALLY: python airflow/scripts/validate_output.py
============================================================
"""

import os
import sys
import csv
from datetime import datetime

# ── Config ─────────────────────────────────────────────────
SCRIPTS_DIR       = os.path.dirname(__file__)
CLEANED_OUTPUT    = os.path.join(SCRIPTS_DIR, "output", "coingecko_cleaned_pyspark")
AGGREGATED_OUTPUT = os.path.join(SCRIPTS_DIR, "output", "coingecko_aggregated_pyspark")
MIN_RECORDS       = 10    # fail if fewer than this many records found


def validate():
    print("=" * 50)
    print("  TASK 3: VALIDATE OUTPUT")
    print("=" * 50)
    print(f"  Time : {datetime.utcnow().isoformat()}Z")

    errors = []

    # ── Check 1: cleaned output folder exists ─────────────
    print(f"\n  [Check 1] Cleaned output folder exists...")
    if not os.path.exists(CLEANED_OUTPUT):
        errors.append(f"FAIL: Cleaned output folder missing: {CLEANED_OUTPUT}")
        print(f"  ✗ {errors[-1]}")
    else:
        print(f"  ✓ Found: {CLEANED_OUTPUT}")

    # ── Check 2: CSV files exist in cleaned output ────────
    print(f"\n  [Check 2] CSV files exist in output...")
    csv_files = []
    if os.path.exists(CLEANED_OUTPUT):
        csv_files = [f for f in os.listdir(CLEANED_OUTPUT) if f.endswith(".csv")]
        if len(csv_files) == 0:
            errors.append("FAIL: No CSV files found in cleaned output folder")
            print(f"  ✗ {errors[-1]}")
        else:
            print(f"  ✓ Found {len(csv_files)} CSV file(s)")

    # ── Check 3: record count in first CSV ───────────────
    print(f"\n  [Check 3] Record count >= {MIN_RECORDS}...")
    if csv_files:
        first_file = os.path.join(CLEANED_OUTPUT, csv_files[0])
        with open(first_file, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            rows = list(reader)
            record_count = len(rows) - 1    # subtract header row

        if record_count < MIN_RECORDS:
            errors.append(f"FAIL: Only {record_count} records found, expected >= {MIN_RECORDS}")
            print(f"  ✗ {errors[-1]}")
        else:
            print(f"  ✓ Record count: {record_count} (>= {MIN_RECORDS})")

    # ── Check 4: aggregated output exists ─────────────────
    print(f"\n  [Check 4] Aggregated output folder exists...")
    if not os.path.exists(AGGREGATED_OUTPUT):
        errors.append(f"FAIL: Aggregated output folder missing: {AGGREGATED_OUTPUT}")
        print(f"  ✗ {errors[-1]}")
    else:
        agg_files = [f for f in os.listdir(AGGREGATED_OUTPUT) if f.endswith(".csv")]
        print(f"  ✓ Found aggregated output with {len(agg_files)} file(s)")

    # ── Final result ───────────────────────────────────────
    print("\n" + "=" * 50)
    if errors:
        print("  VALIDATION FAILED:")
        for e in errors:
            print(f"    ✗ {e}")
        print("=" * 50)
        sys.exit(1)    # exit code 1 = Airflow marks this task as FAILED
    else:
        print("  VALIDATION PASSED — All checks OK")
        print("=" * 50)
        sys.exit(0)    # exit code 0 = Airflow marks this task as SUCCESS


if __name__ == "__main__":
    validate()
