"""
============================================================
Kafka Producer — Simplified Teaching Version
CoinGecko Live Data Ingestion
============================================================
WHAT THIS FILE DOES (3 things only):
  1. Fetch coin prices from CoinGecko API
  2. Convert each coin to a JSON message
  3. Send that message into Kafka topic "crypto-prices"

Then wait 30 seconds and repeat forever.

INSTALL : pip install kafka-python requests
RUN     : python kafka/kafka_producer1.py
============================================================
"""

import json
import time
import requests
from kafka import KafkaProducer

# ============================================================
# PART 1 — CONFIGURATION
# These are the only settings you need to know
# ============================================================

KAFKA_BROKER     = "localhost:9092"   # where Kafka is running
KAFKA_TOPIC      = "crypto-prices"    # the channel we send data into
FETCH_INTERVAL   = 30                 # seconds between each API call

# CoinGecko free public API — no API key needed
API_URL = "https://api.coingecko.com/api/v3/coins/markets"


# ============================================================
# PART 2 — STEP 1: CONNECT TO KAFKA
# KafkaProducer is the object that sends messages to Kafka
# value_serializer converts Python dict → bytes (Kafka needs bytes)
# ============================================================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda data: json.dumps(data).encode("utf-8")
    #                              ↑ dict to JSON string, then to bytes
)

print(f"✓ Connected to Kafka at {KAFKA_BROKER}")
print(f"✓ Will send messages to topic: '{KAFKA_TOPIC}'")
print(f"✓ Fetching every {FETCH_INTERVAL} seconds\n")


# ============================================================
# PART 3 — STEP 2: FETCH DATA FROM COINGECKO API
# Returns a list of 100 coin dicts from the REST API
# ============================================================

def fetch_coins():
    params = {
        "vs_currency" : "usd",          # prices in US dollars
        "order"       : "market_cap_desc",  # top coins first
        "per_page"    : 100,             # fetch top 100 coins
        "page"        : 1,
        "sparkline"   : False,
    }
    response = requests.get(API_URL, params=params, timeout=15)
    coins = response.json()             # list of coin dicts
    return coins


# ============================================================
# PART 4 — STEP 3: SEND EACH COIN TO KAFKA
# producer.send() puts the message into the Kafka topic
# producer.flush() makes sure all messages are delivered
# ============================================================

def send_to_kafka(coins):
    for coin in coins:
        # Each coin becomes ONE Kafka message
        # key  = coin id  (so the same coin always goes to same partition)
        # value = the full coin dict (auto-converted to JSON bytes)
        producer.send(
            KAFKA_TOPIC,
            key   = coin["id"].encode("utf-8"),
            value = coin
        )

    # Flush = "make sure all messages are actually sent"
    producer.flush()
    print(f"  ✓ Sent {len(coins)} coins to Kafka topic '{KAFKA_TOPIC}'")


# ============================================================
# PART 5 — MAIN LOOP
# Runs forever: fetch → send → wait 30 sec → repeat
# Press Ctrl+C to stop
# ============================================================

batch = 0

print("=" * 50)
print("  PRODUCER STARTED — Ctrl+C to stop")
print("=" * 50)

while True:
    batch += 1
    print(f"\n[Batch #{batch}]")

    # Step 1: fetch from API
    print("Fetching from CoinGecko API...")
    coins = fetch_coins()
    print(f"  Got {len(coins)} coins")

    # Step 2: show a quick preview (top 3)
    print("  Preview (top 3):")
    for coin in coins[:3]:
        print(f"    {coin['name']:15s}  ${coin['current_price']:>12,.2f}  "
              f"({coin['price_change_percentage_24h']:+.2f}% 24h)")

    # Step 3: send all coins to Kafka
    send_to_kafka(coins)

    # Step 4: wait before next fetch
    print(f"  Sleeping {FETCH_INTERVAL}s...")
    time.sleep(FETCH_INTERVAL)



    
