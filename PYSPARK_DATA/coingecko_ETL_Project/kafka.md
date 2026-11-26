# Apache Kafka — Real-Time Streaming in CoinGecko ETL Pipeline

## What is Apache Kafka?

Apache Kafka is an open-source **distributed event streaming platform** used for building real-time data pipelines and streaming applications. Think of it as a high-speed, fault-tolerant **conveyor belt** for data — producers put messages on the belt, and consumers pick them up at their own pace.

Originally built by LinkedIn, now maintained by the Apache Software Foundation. Used by Netflix, Uber, Airbnb, and most large-scale data platforms.

---

## Why Kafka for a Crypto ETL Pipeline?

Cryptocurrency prices change **every second**. Your current PySpark ETL reads from a static CSV file — it's a snapshot, not live data. Kafka solves this by continuously streaming price data from the CoinGecko API into your PySpark processor in real time.

| Problem (Batch ETL) | Solution (Kafka + Streaming ETL) |
|---|---|
| CSV is stale the moment it's created | Kafka streams live prices every few seconds |
| No fault tolerance — crash = data loss | Kafka stores messages on disk, supports replay |
| One consumer only | Multiple consumers can read the same topic |
| Must rerun the whole pipeline manually | Pipeline runs continuously, always up to date |

---

## Core Kafka Concepts

### 1. Producer
A Producer is any application that **sends (publishes) data** into Kafka.

In this project:
- A Python script calls the CoinGecko API every 30 seconds
- It formats the response as JSON
- It pushes each record into a Kafka topic called `crypto-prices`

```python
# Simplified Producer concept
producer.send("crypto-prices", value={"coin": "bitcoin", "price": 65000, "ts": "2026-10-12T10:00:00"})
```

---

### 2. Topic
A Topic is a **named channel** where messages are stored and organized — like a table in a database, but for streaming data.

- Topics are split into **Partitions** for parallel processing
- Each message in a partition has an **Offset** (its position/index)
- Multiple topics can exist: `crypto-prices`, `crypto-alerts`, `crypto-aggregated`

```
Topic: "crypto-prices"
├── Partition 0: [msg0, msg1, msg2, msg3, ...]
├── Partition 1: [msg0, msg1, msg2, ...]
└── Partition 2: [msg0, msg1, ...]
```

---

### 3. Consumer
A Consumer is any application that **reads data** from a Kafka topic.

In this project:
- PySpark Structured Streaming reads from the `crypto-prices` topic
- It processes the stream: cleans, transforms, aggregates
- Writes results to Parquet files or a database

```python
# Simplified Consumer concept
df_stream = spark.readStream.format("kafka").option("subscribe", "crypto-prices").load()
```

---

### 4. Broker
A Broker is the **Kafka server** that stores topics and serves producers and consumers. In production you have multiple brokers for high availability. For local development, one broker is enough.

---

### 5. Consumer Group
Multiple consumers can form a **Consumer Group** to share the load of reading from a topic. Each partition is read by only one consumer in the group at a time.

```
Topic: "crypto-prices" (3 partitions)
Consumer Group: "pyspark-etl-group"
├── Consumer 1 → Partition 0
├── Consumer 2 → Partition 1
└── Consumer 3 → Partition 2
```

---

### 6. Offset
The **Offset** is the position of a message within a partition. Consumers track their offset so they know where to resume after a restart — this enables **fault tolerance and replay**.

```
Partition 0: [0: bitcoin $65000] [1: ethereum $3200] [2: bitcoin $65100] ...
                                                      ↑
                                              Consumer is here (offset 2)
```

---

### 7. ZooKeeper / KRaft
Kafka traditionally used **ZooKeeper** for cluster coordination (leader election, config management). Newer Kafka versions (2.8+) support **KRaft mode** — Kafka manages its own metadata without ZooKeeper. For learning purposes, Docker Compose handles this automatically.

---

## How Kafka Fits Into Your ETL — Step by Step

### Current Architecture (Batch)
```
CoinGecko API
     │
     ▼ (manual download)
coingecko_raw_data.csv
     │
     ▼
3_PySpark_ETL_Example.py  (runs once, exits)
     │
     ▼
output/coingecko_aggregated_pyspark/
```

### New Architecture (Streaming with Kafka)
```
CoinGecko API
     │
     ▼ (every 30 seconds, automated)
kafka_producer.py  ──────►  Kafka Broker
                                  │
                            Topic: "crypto-prices"
                            ├── Partition 0
                            ├── Partition 1
                            └── Partition 2
                                  │
                                  ▼
                    PySpark Structured Streaming Consumer
                                  │
                    ┌─────────────┼─────────────┐
                    ▼             ▼             ▼
               Data Clean    Aggregations   Alerting
                    │             │             │
                    ▼             ▼             ▼
              Parquet Files   Dashboard    Alert System
```

---

## Kafka in the ETL Phases

### Extract (E)
- Kafka **replaces the static CSV** as the data source
- Producer continuously extracts from CoinGecko API and pushes to Kafka
- PySpark reads from Kafka topic as a **streaming DataFrame** instead of `spark.read.csv()`

```python
# BEFORE (Batch Extract)
df = spark.read.csv("input/coingecko_raw_data.csv", header=True, inferSchema=True)

# AFTER (Streaming Extract from Kafka)
df_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "crypto-prices") \
    .option("startingOffsets", "latest") \
    .load()
```

---

### Transform (T)
- All your existing PySpark transformations work **the same way** on streaming DataFrames
- The key difference: transformations run on **micro-batches** (small chunks of records every few seconds)
- Windowed aggregations let you compute stats like "average price in last 5 minutes"

```python
# Windowed aggregation — unique to streaming
from pyspark.sql.functions import window

windowed_avg = df_stream \
    .groupBy(
        window("timestamp", "5 minutes"),  # 5-minute window
        "coin_name"
    ).agg(
        F.avg("current_price").alias("avg_price_5min"),
        F.max("current_price").alias("max_price_5min")
    )
```

---

### Load (L)
- Results are written as a **continuous stream** to the sink
- Supports Parquet, CSV, Delta Lake, databases, or back to another Kafka topic

```python
# Streaming write to Parquet
query = df_clean_stream \
    .writeStream \
    .format("parquet") \
    .option("checkpointLocation", "output/checkpoints/") \
    .option("path", "output/crypto_live_parquet/") \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .start()

query.awaitTermination()
```

---

## Key Kafka Properties for ETL

### Message Retention
Kafka keeps messages for a configurable time (default: 7 days). This means:
- You can **replay** past data at any time
- If your PySpark job was down for 2 hours, it can catch up from where it left off
- Useful for debugging, reprocessing, and auditing

### At-Least-Once Delivery
By default Kafka guarantees **at-least-once delivery** — every message is delivered at least once. Combined with PySpark checkpointing, you get **exactly-once** processing semantics.

### Throughput
Kafka handles **millions of messages per second**. For crypto data, even at 1000 coins × every second = 1000 msg/sec — Kafka handles this with ease.

---

## Comparison: Batch vs Streaming ETL

| Aspect | Batch ETL (Current) | Kafka Streaming ETL |
|---|---|---|
| Data freshness | Hours/days old | Seconds old |
| Trigger | Manual / scheduled | Continuous |
| Fault tolerance | None | Offset-based replay |
| Scalability | Limited by single machine | Horizontally scalable |
| Data source | Static CSV file | Live API → Kafka topic |
| PySpark API | `spark.read` | `spark.readStream` |
| Output | Fixed files | Continuously updated |
| Complexity | Low | Medium |
| Industry use | Learning / small data | Production pipelines |

---

## Files You Would Create to Implement This

```
coingecko_ETL_Project/
├── input/
│   └── coingecko_raw_data.csv          ← existing (batch)
├── output/                              ← existing
├── kafka/
│   ├── docker-compose.yml              ← starts Kafka + ZooKeeper locally
│   ├── kafka_producer.py               ← calls CoinGecko API, sends to Kafka
│   └── kafka_consumer_pyspark.py       ← PySpark Structured Streaming consumer
├── 1_Pandas_vs_PySpark_Comparison.md   ← existing
├── 2_Pandas_ETL_Example.py             ← existing
├── 3_PySpark_ETL_Example.py            ← existing (batch)
├── 4_Teaching_Guide.md                 ← existing
├── 5_Quick_Start_Guide.md              ← existing
├── 6_Student_Exercises.md              ← existing
└── kafka.md                            ← THIS FILE
```

---

## Setup Overview (When You Are Ready)

### Step 1 — Install Docker Desktop
Kafka is easiest to run locally via Docker. No manual installation needed.

### Step 2 — docker-compose.yml
A single file that spins up Kafka + ZooKeeper:
```yaml
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
```
Run with: `docker-compose up -d`

### Step 3 — kafka_producer.py
Python script that:
1. Calls CoinGecko API every 30 seconds
2. Serializes response to JSON
3. Sends to Kafka topic `crypto-prices`

Install: `pip install kafka-python requests`

### Step 4 — kafka_consumer_pyspark.py
PySpark Structured Streaming script that:
1. Reads from Kafka topic `crypto-prices`
2. Parses JSON messages
3. Applies same transformations as `3_PySpark_ETL_Example.py`
4. Writes results to Parquet every 30 seconds

Install: `pip install pyspark` + add Kafka connector JAR

### Step 5 — Run everything
```
Terminal 1: docker-compose up -d          # start Kafka
Terminal 2: python kafka_producer.py      # start pushing data
Terminal 3: python kafka_consumer_pyspark.py  # start processing
```

---

## Real-World Use Cases for Kafka in Crypto

| Use Case | Description |
|---|---|
| Live price monitoring | Stream prices every second, alert on sudden drops |
| Fraud detection | Detect abnormal trading volumes in real time |
| Portfolio tracking | Continuously update portfolio value as prices change |
| Arbitrage detection | Compare prices across exchanges in real time |
| Market sentiment | Stream social media + prices together |
| Regulatory reporting | Audit trail of all price changes with timestamps |

---

## Key Takeaways

1. **Kafka is the bridge** between your data source (CoinGecko API) and your processor (PySpark)
2. **It decouples** the producer (API caller) from the consumer (ETL processor) — they run independently
3. **Fault tolerance** comes from offset tracking — no data is lost on failure
4. **Your existing PySpark code** is mostly reusable — you just swap `spark.read` for `spark.readStream`
5. **Windowed aggregations** are the new concept — compute stats over time windows (1 min, 5 min, 1 hour)
6. This pattern is used by **every major data platform** — Netflix, Uber, Airbnb, LinkedIn all use Kafka at the core of their data pipelines

---

## Further Reading

- [Apache Kafka Official Documentation](https://kafka.apache.org/documentation/)
- [PySpark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Confluent Kafka Python Client](https://docs.confluent.io/kafka-clients/python/current/overview.html)
- [Spark + Kafka Integration](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
