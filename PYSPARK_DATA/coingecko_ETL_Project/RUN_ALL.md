# Run Everything — Kafka + Airflow Together

## All Ports at a Glance

| Service | URL / Port | Purpose |
|---|---|---|
| Kafka Broker | `localhost:9092` | Producer & PySpark connect here |
| Kafka UI | http://localhost:8080 | Browse topics and messages |
| Airflow UI | http://localhost:8081 | Manage DAGs, view logs |
| PostgreSQL | `localhost:5432` | Airflow internal database |

---

## One-Time Setup — Copy Scripts into Airflow

Run this ONCE from the project root:

```
copy 3_PySpark_ETL_Example.py airflow\scripts\
xcopy input airflow\scripts\input /E /I
```

---

## Start Everything (1 command)

```bash
# From project root (where docker-compose.yml is)
docker-compose up -d
```

Wait ~30 seconds for all services to start.

Check all containers are running:
```bash
docker-compose ps
```

Expected output:
```
NAME                  STATUS
zookeeper             running
kafka                 running
kafka_ui              running
airflow_postgres      running
airflow_init          exited (0)     ← this is normal, it runs once and exits
airflow_webserver     running
airflow_scheduler     running
```

---

## Start Services One at a Time (Optional)

If you want more control:

```bash
# Start only Kafka stack
docker-compose up -d zookeeper kafka kafka-ui

# Start only Airflow stack
docker-compose up -d postgres airflow-init airflow-webserver airflow-scheduler
```

---

## Run the Python Scripts (after Docker is up)

Open separate terminals for each:

```bash
# Terminal 1 — Kafka Producer (live data every 30 sec)
python kafka/kafka_producer1.py

# Terminal 2 — PySpark Kafka Consumer (processes the stream)
python kafka/kafka_pyspark_consumer1.py

# Terminal 3 — OR run the batch ETL manually (no Kafka needed)
python 3_PySpark_ETL_Example.py
```

Airflow runs `3_PySpark_ETL_Example.py` automatically on its schedule (every 15 min).
You don't need to run Terminal 3 manually once Airflow is set up.

---

## Open the UIs

```
Kafka UI   → http://localhost:8080
             (see topic "crypto-prices", browse messages)

Airflow UI → http://localhost:8081
             Username: admin
             Password: admin
             (enable DAG "coingecko_etl_pipeline", click ▶ to run)
```

---

## View Logs

```bash
# Logs from all services
docker-compose logs -f

# Logs from Kafka only
docker-compose logs -f kafka

# Logs from Airflow scheduler only
docker-compose logs -f airflow-scheduler

# Logs from Airflow webserver only
docker-compose logs -f airflow-webserver
```

---

## Stop Everything

```bash
docker-compose down
```

Stop and remove all data (full reset):
```bash
docker-compose down -v
```

---

## Restart a Single Service

```bash
docker-compose restart kafka
docker-compose restart airflow-scheduler
docker-compose restart airflow-webserver
```

---

## Full Architecture When Running

```
┌──────────────────────────────────────────────────────────┐
│                    YOUR MACHINE                           │
│                                                          │
│  ┌─────────────────────────────────────────────────┐    │
│  │              DOCKER CONTAINERS                   │    │
│  │                                                  │    │
│  │  [ZooKeeper :2181]                               │    │
│  │       │                                          │    │
│  │  [Kafka :9092] ←── kafka_producer1.py            │    │
│  │       │         ──► kafka_pyspark_consumer1.py   │    │
│  │  [Kafka UI :8080]  http://localhost:8080         │    │
│  │                                                  │    │
│  │  [PostgreSQL :5432]                              │    │
│  │       │                                          │    │
│  │  [Airflow Webserver :8081]  http://localhost:8081│    │
│  │  [Airflow Scheduler]                             │    │
│  │       │ (every 15 min triggers DAG)              │    │
│  │       │                                          │    │
│  │       ├── Task 1: fetch_coingecko.py             │    │
│  │       ├── Task 2: 3_PySpark_ETL_Example.py       │    │
│  │       └── Task 3: validate_output.py             │    │
│  └─────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────┘
```
