# How to Run Airflow — CoinGecko ETL Pipeline

## Folder Structure
```
airflow/
├── docker-compose.yml          ← starts Airflow + PostgreSQL via Docker
├── dags/
│   └── coingecko_dag.py        ← pipeline definition (3 tasks, schedule, retry)
├── scripts/
│   ├── fetch_coingecko.py      ← Task 1: calls CoinGecko API, saves CSV
│   ├── 3_PySpark_ETL_Example.py← Task 2: your existing PySpark ETL (copy here)
│   └── validate_output.py      ← Task 3: checks output files and record count
└── logs/                       ← Airflow stores task logs here (auto-created)
```

---

## Step 1 — Copy your PySpark script into airflow/scripts/

```
copy 3_PySpark_ETL_Example.py  airflow\scripts\3_PySpark_ETL_Example.py
```

Also copy the input folder:
```
xcopy input airflow\scripts\input /E /I
```

---

## Step 2 — Start Airflow via Docker

```bash
cd airflow
docker-compose up -d
```

Wait about 30 seconds. Then check all containers are running:
```bash
docker ps
```
You should see 3 containers:
- airflow_postgres
- airflow_webserver
- airflow_scheduler

---

## Step 3 — Open the Web UI

Open your browser → http://localhost:8081

```
Username: admin
Password: admin
```

You will see the DAG list page with `coingecko_etl_pipeline` listed.

---

## Step 4 — Enable and Trigger the DAG

1. Toggle the DAG switch to **ON** (it starts scheduling automatically)
2. Click the ▶ (play) button to trigger a manual run immediately
3. Click the DAG name → **Grid View** to watch tasks run

---

## What You See in the UI

```
Grid View — each column is one DAG run:

Task              Run 1    Run 2    Run 3
──────────────────────────────────────────
fetch_data         ✓ green  ✓ green  ✗ red
run_pyspark_etl    ✓ green  ✓ green   (skipped — task 1 failed)
validate_output    ✓ green  ✓ green   (skipped)

Click any colored box → View Logs → see full Python output
```

---

## Schedule Options (edit coingecko_dag.py)

| Change to | schedule_interval value |
|---|---|
| Every 15 minutes | `"*/15 * * * *"` |
| Every hour | `"@hourly"` |
| Every 6 hours | `"0 */6 * * *"` |
| Every day at midnight | `"@daily"` |
| Every Monday 9am | `"0 9 * * 1"` |

---

## Retry Behavior

If any task fails:
```
Task fails
  → Wait 5 minutes
  → Retry attempt 1
  → Wait 5 minutes
  → Retry attempt 2
  → Wait 5 minutes
  → Retry attempt 3
  → If still failing → mark FAILED (shown red in UI)
```

View retry logs: click red task → Logs → use attempt dropdown to see each retry.

---

## Stop Airflow

```bash
docker-compose down
```
