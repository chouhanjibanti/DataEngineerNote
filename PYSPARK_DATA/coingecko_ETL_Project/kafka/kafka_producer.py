"""
============================================================
Kafka Producer — CoinGecko Live Data Ingestion
============================================================
PURPOSE  : Fetches top 100 coins from CoinGecko API every
           30 seconds and pushes each coin as a JSON message
           into the Kafka topic  "crypto-prices"

REPLACES : input/coingecko_raw_data.csv  (static file)

INSTALL  : pip install kafka-python requests
RUN      : python kafka/kafka_producer.py
============================================================
"""

import json
import time
import logging
import requests
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ── Logging setup ──────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [PRODUCER]  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────
KAFKA_BROKER      = "localhost:9092"
KAFKA_TOPIC       = "crypto-prices"
FETCH_INTERVAL_S  = 30          # seconds between each API call
COINS_PER_PAGE    = 100         # top N coins by market cap
CURRENCY          = "usd"

# CoinGecko free API — no API key required
COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"
API_PARAMS = {
    "vs_currency"            : CURRENCY,
    "order"                  : "market_cap_desc",
    "per_page"               : COINS_PER_PAGE,
    "page"                   : 1,
    "sparkline"              : False,
    "price_change_percentage": "24h",
}

# ── Helper: serialise Python dict → bytes ─────────────────
def json_serializer(data: dict) -> bytes:
    return json.dumps(data, default=str).encode("utf-8")


# ── Helper: fetch data from CoinGecko ─────────────────────
def fetch_coingecko_data() -> list:
    """
    Calls the CoinGecko /coins/markets endpoint.
    Returns a list of coin dicts that match the CSV columns
    used in 3_PySpark_ETL_Example.py.
    """
    try:
        response = requests.get(COINGECKO_URL, params=API_PARAMS, timeout=15)
        response.raise_for_status()
        coins = response.json()
        log.info(f"Fetched {len(coins)} coins from CoinGecko API")
        return coins
    except requests.exceptions.HTTPError as e:
        log.error(f"HTTP error from CoinGecko: {e}")
    except requests.exceptions.ConnectionError:
        log.error("Cannot reach CoinGecko API — check your internet connection")
    except requests.exceptions.Timeout:
        log.error("CoinGecko API request timed out")
    except Exception as e:
        log.error(f"Unexpected error fetching data: {e}")
    return []


# ── Helper: map API response → flat record ────────────────
def map_coin_to_record(coin: dict) -> dict:
    """
    Maps the raw CoinGecko API response fields to the same
    column names used in the static CSV / PySpark ETL.
    Adds a 'kafka_ingested_at' timestamp for traceability.
    """
    return {
        # ── identifiers ────────────────────────────────────
        "id"                              : coin.get("id"),
        "symbol"                          : coin.get("symbol"),
        "name"                            : coin.get("name"),
        "image"                           : coin.get("image"),

        # ── price & volume ─────────────────────────────────
        "current_price"                   : coin.get("current_price"),
        "market_cap"                      : coin.get("market_cap"),
        "market_cap_rank"                 : coin.get("market_cap_rank"),
        "fully_diluted_valuation"         : coin.get("fully_diluted_valuation"),
        "total_volume"                    : coin.get("total_volume"),
        "high_24h"                        : coin.get("high_24h"),
        "low_24h"                         : coin.get("low_24h"),
        "price_change_24h"                : coin.get("price_change_24h"),
        "price_change_percentage_24h"     : coin.get("price_change_percentage_24h"),
        "market_cap_change_24h"           : coin.get("market_cap_change_24h"),
        "market_cap_change_percentage_24h": coin.get("market_cap_change_percentage_24h"),

        # ── supply ─────────────────────────────────────────
        "circulating_supply"              : coin.get("circulating_supply"),
        "total_supply"                    : coin.get("total_supply"),
        "max_supply"                      : coin.get("max_supply"),

        # ── all-time high/low ──────────────────────────────
        "ath"                             : coin.get("ath"),
        "ath_change_percentage"           : coin.get("ath_change_percentage"),
        "ath_date"                        : coin.get("ath_date"),
        "atl"                             : coin.get("atl"),
        "atl_change_percentage"           : coin.get("atl_change_percentage"),
        "atl_date"                        : coin.get("atl_date"),

        # ── metadata ───────────────────────────────────────
        "roi"                             : coin.get("roi"),
        "last_updated"                    : coin.get("last_updated"),

        # ── Kafka metadata (extra field — not in CSV) ──────
        "kafka_ingested_at"               : datetime.utcnow().isoformat() + "Z",
    }


# ── Main producer loop ────────────────────────────────────
def main():
    log.info("=" * 55)
    log.info("  COINGECKO KAFKA PRODUCER  STARTING")
    log.info("=" * 55)
    log.info(f"  Broker  : {KAFKA_BROKER}")
    log.info(f"  Topic   : {KAFKA_TOPIC}")
    log.info(f"  Interval: every {FETCH_INTERVAL_S} seconds")
    log.info(f"  Coins   : top {COINS_PER_PAGE} by market cap")
    log.info("=" * 55)

    # ── Connect to Kafka ───────────────────────────────────
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=json_serializer,
            # Retry up to 3 times on transient failures
            retries=3,
            # Wait for leader acknowledgement
            acks="all",
        )
        log.info(f"✓ Connected to Kafka broker at {KAFKA_BROKER}")
    except KafkaError as e:
        log.error(f"✗ Could not connect to Kafka: {e}")
        log.error("  Make sure Kafka is running:  docker-compose up -d")
        return

    batch_number = 0

    try:
        while True:
            batch_number += 1
            log.info(f"── Batch #{batch_number} ──────────────────────────────────")

            # 1. Fetch from API
            coins = fetch_coingecko_data()

            if not coins:
                log.warning("No data received — skipping this batch")
            else:
                sent = 0
                errors = 0

                # 2. Send each coin as an individual Kafka message
                for coin in coins:
                    record = map_coin_to_record(coin)
                    try:
                        # key = coin id (bytes) → ensures same coin → same partition
                        producer.send(
                            KAFKA_TOPIC,
                            key=record["id"].encode("utf-8"),
                            value=record,
                        )
                        sent += 1
                    except KafkaError as e:
                        log.error(f"  Failed to send {coin.get('id')}: {e}")
                        errors += 1

                # 3. Flush — ensure all messages are delivered
                producer.flush()

                log.info(f"  ✓ Sent {sent} messages | Errors: {errors}")

                # Quick preview of top 5
                log.info("  Preview (top 5 by market cap):")
                for c in coins[:5]:
                    log.info(
                        f"    {c.get('name'):20s} "
                        f"${c.get('current_price'):>12,.2f}  "
                        f"({c.get('price_change_percentage_24h'):+.2f}% 24h)"
                    )

            # 4. Wait before next fetch
            log.info(f"  Sleeping {FETCH_INTERVAL_S}s until next batch...")
            time.sleep(FETCH_INTERVAL_S)

    except KeyboardInterrupt:
        log.info("\n⏹  Producer stopped by user (Ctrl+C)")
    finally:
        producer.close()
        log.info("✓ Kafka producer closed cleanly")


if __name__ == "__main__":
    main()
