# Apache Kafka Integration — Full Explanation for CoinGecko ETL Project

---

## Can Kafka Be Integrated Into This Project?

**Yes — Apache Kafka CAN be integrated into this project, and it makes a lot of sense.**

---

## Step 1: What is Your Current ETL Flow?

Your `3_PySpark_ETL_Example.py` does this:

```
CSV file (static)  →  PySpark reads it  →  Clean/Transform  →  Write to CSV output
```

This is a **batch ETL** — it runs once, processes a fixed file, and stops.  
Good for learning, but **not how production crypto data pipelines work**.

---

## Step 2: What is Apache Kafka?

Kafka is a **distributed message streaming platform**. Think of it as a high-speed, fault-tolerant **conveyor belt** for data.

```
Producer (sends data)  →  Kafka Topic (stores messages)  →  Consumer (reads data)
```

- **Producer** = something that pushes data in (e.g., a script calling CoinGecko API every 10 seconds)
- **Topic** = a named channel/queue (e.g., `crypto-prices`)
- **Consumer** = something that reads from that topic (e.g., your PySpark job)
- **Broker** = the Kafka server that manages topics

---

## Step 3: Where Does Kafka Fit in YOUR CoinGecko ETL?

Right now your pipeline is **pull-based and static**. With Kafka it becomes **event-driven and real-time**.

### Before (Batch):
```
CoinGecko API  →  [You manually download CSV]  →  PySpark reads CSV  →  Output
```

### After (Streaming with Kafka):
```
CoinGecko API  →  Kafka Producer  →  Kafka Topic "crypto-prices"  →  PySpark Structured Streaming  →  Output (live)
```

---

## Step 4: The 4 Main Purposes Kafka Serves in an ETL Pipeline

### 1. Real-time Data Ingestion (Extract Layer)
Instead of reading a static CSV, a Kafka Producer hits the CoinGecko API every N seconds and pushes each price update as a message into a Kafka topic. Your ETL always has **fresh data**.

### 2. Decoupling Producer and Consumer
The API caller and the PySpark processor work **independently**. If PySpark is busy, messages queue up in Kafka. Nothing is lost. This is called **backpressure handling**.

### 3. Fault Tolerance & Replay
Kafka stores messages on disk for a configurable period (e.g., 7 days). If your PySpark job crashes, it can **replay from the last checkpoint** — no data loss.

### 4. Scalability for Multiple Consumers
Multiple systems can consume the same Kafka topic simultaneously:
- PySpark does aggregations → writes to database
- An alert service watches for price drops > 10%
- A dashboard reads live prices

---

## Step 5: How the New Architecture Would Look in Your Project

```
┌─────────────────────────────────────────────────────────────┐
│                    KAFKA + PYSPARK ETL                       │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  CoinGecko API                                               │
│       │                                                      │
│       ▼  (every 30 seconds)                                  │
│  kafka_producer.py  ──────►  Kafka Topic: "crypto-prices"   │
│                                      │                       │
│                                      ▼                       │
│                           PySpark Structured Streaming       │
│                           (reads from Kafka topic)           │
│                                      │                       │
│                            ┌─────────┴──────────┐           │
│                            ▼                    ▼            │
│                     Clean/Transform        Aggregations      │
│                            │                    │            │
│                            ▼                    ▼            │
│                       Parquet/DB           Dashboard/Alert   │
└─────────────────────────────────────────────────────────────┘
```

---

## Step 6: What You'd Need to Implement This

| Component | Tool |
|---|---|
| Kafka server | Docker (easiest) or local install |
| Producer script | Python `kafka-python` or `confluent-kafka` |
| Consumer/Processor | PySpark Structured Streaming + `spark-sql-kafka` connector |
| API source | CoinGecko free API (already in your project) |
| Output sink | CSV, Parquet, PostgreSQL, or console |

---

## Step 7: Is It Practical for Your Project Specifically?

**Honest answer:**

- **For learning data engineering** → Yes, very worth it. Kafka + PySpark Streaming is a core skill in the industry.
- **For production CoinGecko monitoring** → Yes, crypto prices change every second, batch ETL misses that.
- **For just processing the existing CSV** → No need. Kafka is overkill for static files.

The key upgrade is replacing your static `coingecko_raw_data.csv` with a **live Kafka stream** that calls the real CoinGecko API continuously.

---

## Summary

| Aspect | Current (Batch) | With Kafka (Streaming) |
|---|---|---|
| Data freshness | As old as the CSV | Near real-time (seconds) |
| Fault tolerance | None | Built-in (replay) |
| Scalability | Single file | Millions of events/sec |
| Complexity | Simple | Medium |
| Industry relevance | Basic | Production-grade |

---

## What Comes Next — Implementation Files

When you are ready to implement, these are the exact files that will be created:

```
coingecko_ETL_Project/
└── kafka/
    ├── docker-compose.yml           ← spins up Kafka locally via Docker
    ├── kafka_producer.py            ← calls CoinGecko API, pushes to Kafka topic
    └── kafka_consumer_pyspark.py    ← PySpark Structured Streaming consumer
```

### docker-compose.yml — Start Kafka Locally
```yaml
version: '3'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka:
    image: confluentinc/cp-kafka:7.4.0
    depends_on: [zookeeper]
    ports:
      - "9092:9092"
    environment:
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
```

Run with: `docker-compose up -d`

---

### kafka_producer.py — Push CoinGecko Data into Kafka
```python
import requests
import json
import time
from kafka import KafkaProducer

# Connect to Kafka broker
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_and_send():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {"vs_currency": "usd", "order": "market_cap_desc", "per_page": 100}
    response = requests.get(url, params=params)
    coins = response.json()

    for coin in coins:
        producer.send("crypto-prices", value=coin)
        print(f"Sent: {coin['name']} @ ${coin['current_price']}")

    producer.flush()

# Run every 30 seconds
while True:
    fetch_and_send()
    time.sleep(30)
```

Install: `pip install kafka-python requests`

---

### kafka_consumer_pyspark.py — PySpark Reads from Kafka
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("CoinGeckoKafkaStream") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .getOrCreate()

# Read stream from Kafka topic
df_raw = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "crypto-prices") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON value from Kafka message
schema = StructType([
    StructField("id", StringType()),
    StructField("name", StringType()),
    StructField("symbol", StringType()),
    StructField("current_price", DoubleType()),
    StructField("market_cap", LongType()),
    StructField("total_volume", LongType()),
    StructField("price_change_percentage_24h", DoubleType())
])

df_parsed = df_raw.select(
    F.from_json(F.col("value").cast("string"), schema).alias("data")
).select("data.*")

# Apply transformations (same logic as 3_PySpark_ETL_Example.py)
df_clean = df_parsed.filter(F.col("current_price") > 0)

df_transformed = df_clean.withColumn(
    "market_cap_category",
    F.when(F.col("market_cap") >= 100000000000, "Large Cap")
     .when(F.col("market_cap") >= 10000000000, "Mid Cap")
     .when(F.col("market_cap") >= 1000000000, "Small Cap")
     .otherwise("Micro Cap")
).withColumn(
    "price_change_category",
    F.when(F.col("price_change_percentage_24h") >= 5, "Strong Positive")
     .when(F.col("price_change_percentage_24h") >= 0, "Positive")
     .when(F.col("price_change_percentage_24h") >= -5, "Negative")
     .otherwise("Strong Negative")
).withColumn("processed_at", F.current_timestamp())

# Write stream to Parquet output
query = df_transformed \
    .writeStream \
    .format("parquet") \
    .option("checkpointLocation", "output/checkpoints/kafka_stream/") \
    .option("path", "output/crypto_live_parquet/") \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .start()

query.awaitTermination()
```

---

### How to Run Everything

```
# Terminal 1 — Start Kafka
docker-compose up -d

# Terminal 2 — Start Producer (pushes CoinGecko data every 30 sec)
python kafka/kafka_producer.py

# Terminal 3 — Start PySpark Consumer (processes the stream)
python kafka/kafka_consumer_pyspark.py
```

---

## Batch vs Streaming — Side-by-Side Code Comparison

### Extract Phase
```python
# BATCH (current 3_PySpark_ETL_Example.py)
df = spark.read.csv("input/coingecko_raw_data.csv", header=True, inferSchema=True)

# STREAMING (with Kafka)
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "crypto-prices") \
    .load()
```

### Transform Phase
```python
# BOTH batch and streaming use the same transformation code
df_clean = df.filter(F.col("current_price") > 0)
df_clean = df_clean.withColumn("market_cap_category", F.when(...))
# No change needed here — PySpark handles both the same way
```

### Load Phase
```python
# BATCH
df_clean.write.mode("overwrite").csv("output/coingecko_cleaned_pyspark")

# STREAMING
df_clean.writeStream.format("parquet") \
    .option("checkpointLocation", "output/checkpoints/") \
    .outputMode("append") \
    .start()
```

---

## Quick Reference — Key Differences to Remember

| Concept | Batch ETL | Kafka Streaming ETL |
|---|---|---|
| API | `spark.read` | `spark.readStream` |
| Data source | Static CSV file | Live Kafka topic |
| Write API | `.write.mode()` | `.writeStream.outputMode()` |
| Termination | Stops automatically | Runs until stopped |
| Fault recovery | Reruns whole job | Resumes from checkpoint offset |
| New concept | None | Windowed aggregations, micro-batches |


---

## Producer vs Consumer — Why Two Separate Files?

### The Simple Analogy

Think of a **postal system**:

```
kafka_producer.py          =  The POST OFFICE  (collects & sends letters)
Kafka Topic                =  The MAILBOX      (stores letters in transit)
kafka_pyspark_consumer.py  =  The RECIPIENT    (receives & reads letters)
```

They are intentionally separate so they can run **independently**.

---

### Side-by-Side Comparison

| | `kafka_producer.py` | `kafka_pyspark_consumer.py` |
|---|---|---|
| **Role** | Sends data INTO Kafka | Reads data OUT OF Kafka |
| **Job** | Fetch from CoinGecko API | Process + transform + save |
| **Library** | `kafka-python`, `requests` | `pyspark` |
| **Talks to** | CoinGecko API → Kafka | Kafka → Parquet files |
| **Runs** | Every 30 seconds (loop) | Continuously listening |
| **Knows about PySpark?** | ❌ No | ✅ Yes |
| **Knows about CoinGecko API?** | ✅ Yes | ❌ No |
| **Output** | Messages into Kafka topic | Cleaned Parquet files |

---

### What Each File Does — In One Line

```
kafka_producer.py          →   "Hey Kafka, here is fresh coin data"
kafka_pyspark_consumer.py  →   "Hey Kafka, give me that data so I can process it"
```

---

### Why NOT Combine Them Into One File?

This is the core idea behind Kafka — **decoupling**. If you merged them:

- If PySpark crashes → the API fetching also stops → **data loss**
- If the API is slow → PySpark sits idle waiting → **wasted resources**
- You can't scale them independently

With 2 separate files:

```
Producer crashes?   → Kafka still holds all buffered messages safely
Consumer crashes?   → Producer keeps running, messages queue up in Kafka
Consumer is slow?   → Producer doesn't care, it keeps sending at its own pace
```

---

### Visual Flow

```
┌─────────────────────┐        ┌──────────────┐        ┌──────────────────────────┐
│  kafka_producer.py  │        │ Kafka Topic  │        │ kafka_pyspark_consumer   │
│                     │        │              │        │                          │
│  1. Call CoinGecko  │──msg──►│ crypto-prices│──msg──►│  1. Read from Kafka      │
│  2. Format as JSON  │        │              │        │  2. Clean data           │
│  3. Send to Kafka   │        │ (acts as     │        │  3. Transform            │
│  4. Wait 30 sec     │        │  a buffer)   │        │  4. Aggregate            │
│  5. Repeat forever  │        │              │        │  5. Save to Parquet      │
└─────────────────────┘        └──────────────┘        └──────────────────────────┘

     EXTRACT (E)                   BUFFER                  TRANSFORM + LOAD (T+L)
```

---

### Real-World Reason

In production companies, these are often maintained by **different teams**:

- The **data ingestion team** owns the producer
- The **data engineering team** owns the consumer

One team can update their side without breaking the other — because Kafka sits in between as the **contract** between them.
