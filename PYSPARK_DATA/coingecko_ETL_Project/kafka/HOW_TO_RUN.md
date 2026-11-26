# How to Run the Kafka + PySpark Live ETL

## Folder Structure Created
```
coingecko_ETL_Project/
└── kafka/
    ├── docker-compose.yml          ← starts Kafka + ZooKeeper + UI
    ├── kafka_producer.py           ← fetches CoinGecko API every 30s → Kafka
    ├── kafka_pyspark_consumer.py   ← PySpark reads Kafka → transforms → Parquet
    ├── requirements.txt            ← Python dependencies
    └── HOW_TO_RUN.md               ← this file
```

---

## Prerequisites

| Tool | Install |
|---|---|
| Docker Desktop | https://www.docker.com/products/docker-desktop |
| Python 3.8+ | already installed |
| pip packages | `pip install -r kafka/requirements.txt` |

---

## Step 1 — Install Python dependencies

```
cd coingecko_ETL_Project
python pip install -r kafka/requirements.txt
```

---

## Step 2 — Start Kafka (Docker)

```
cd kafka
docker-compose up -d
```

Wait ~15 seconds for Kafka to be ready.

Optional: open http://localhost:8080 in your browser to see the Kafka UI dashboard.

---

## Step 3 — Start the Producer (Terminal 1)

```
python kafka/kafka_producer.py
```

You will see output like:
```
2026-10-12 10:00:00  [PRODUCER]  Fetched 100 coins from CoinGecko API
2026-10-12 10:00:01  [PRODUCER]  ✓ Sent 100 messages | Errors: 0
2026-10-12 10:00:01  [PRODUCER]  Preview (top 5 by market cap):
2026-10-12 10:00:01  [PRODUCER]    Bitcoin              $  64,407.00  (-0.90% 24h)
2026-10-12 10:00:01  [PRODUCER]    Ethereum             $   1,894.25  (-1.20% 24h)
...
2026-10-12 10:00:01  [PRODUCER]  Sleeping 30s until next batch...
```

---

## Step 4 — Start the PySpark Consumer (Terminal 2)

```
python kafka/kafka_pyspark_consumer.py
```

Every 30 seconds you will see:
```
============================================================
[Batch #1]  100 records received from Kafka
============================================================

[STEP 5] TOP 10 COINS (this batch):
+--------+------+-------------+---------------+------------------+--------------------------+
|name    |symbol|current_price|market_cap_cat |price_change_cat  |price_change_percentage_24h|
+--------+------+-------------+---------------+------------------+--------------------------+
|Bitcoin |btc   |64407.0      |Large Cap      |Negative          |-0.9                      |
|Ethereum|eth   |1894.25      |Large Cap      |Negative          |-1.2                      |
...

[STEP 5] MARKET CAP CATEGORY SUMMARY (this batch):
+------------------+----------+---------+------------+
|market_cap_category|coin_count|avg_price|total_mcap_B|
+------------------+----------+---------+------------+
|Large Cap          |8         |12450.23 |2450.12     |
|Mid Cap            |15        |234.56   |312.45      |
...

[STEP 6] DATA VALIDATION:
  ✓ Negative prices     : 0
  ✓ Zero market cap     : 0
  ✓ Records after clean : 99

[STEP 7] SAVING batch #1 → output/kafka_live_parquet
  ✓ Saved 99 records
```

---

## Output Files

```
coingecko_ETL_Project/
└── output/
    ├── kafka_live_parquet/
    │   ├── market_cap_category=Large Cap/
    │   │   └── part-00000-xxx.parquet
    │   ├── market_cap_category=Mid Cap/
    │   │   └── part-00000-xxx.parquet
    │   ├── market_cap_category=Small Cap/
    │   │   └── part-00000-xxx.parquet
    │   └── market_cap_category=Micro Cap/
    │       └── part-00000-xxx.parquet
    └── checkpoints/
        └── kafka_stream/           ← fault tolerance — resume from here on crash
```

---

## Stop Everything

```
# Stop producer:  Ctrl+C in Terminal 1
# Stop consumer:  Ctrl+C in Terminal 2

# Stop Kafka:
docker-compose down
```

---

## What Changed vs 3_PySpark_ETL_Example.py

| | Batch (original) | Kafka Streaming (new) |
|---|---|---|
| Data source | `spark.read.csv(...)` | `spark.readStream.format("kafka")` |
| Runs | Once then exits | Forever (every 30 sec) |
| Cleaning | Step 3 | Same logic in `apply_cleaning()` |
| Transform | Step 4 | Same logic in `apply_transformations()` |
| Write | `.write.mode("overwrite")` | `.writeStream.foreachBatch()` |
| Output format | CSV | Parquet (partitioned by category) |
| Fault recovery | None | Checkpoint-based offset tracking |
| Live aggregation | Not possible | Windowed aggregation (5-min window) |



API :- 
https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100&page=1&sparkline=false&price_change_percentage=24h