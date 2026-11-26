"""
============================================================
Task 1 Script — Fetch CoinGecko API Data
============================================================
Called by Airflow DAG:  coingecko_etl_pipeline
Task name            :  fetch_coingecko_data

WHAT IT DOES:
  - Calls CoinGecko /coins/markets API
  - Saves top 100 coins as CSV
  - Output used by Task 2 (PySpark ETL)

RUN MANUALLY: python airflow/scripts/fetch_coingecko.py
============================================================
"""

import os
import csv
import sys
import requests
from datetime import datetime

# ── Config ─────────────────────────────────────────────────
API_URL    = "https://api.coingecko.com/api/v3/coins/markets"
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "input")
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "coingecko_raw_data.csv")

API_PARAMS = {
    "vs_currency" : "usd",
    "order"       : "market_cap_desc",
    "per_page"    : 100,
    "page"        : 1,
    "sparkline"   : False,
}

# ── Main fetch function ────────────────────────────────────
def fetch_and_save():
    print("=" * 50)
    print("  TASK 1: FETCH COINGECKO DATA")
    print("=" * 50)
    print(f"  Time      : {datetime.utcnow().isoformat()}Z")
    print(f"  Output    : {OUTPUT_CSV}")

    # Step 1 — Call the API
    print("\n  Calling CoinGecko API...")
    response = requests.get(API_URL, params=API_PARAMS, timeout=15)
    response.raise_for_status()          # raises exception if API returns error
    coins = response.json()
    print(f"  ✓ Received {len(coins)} coins")

    # Step 2 — Save to CSV
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=coins[0].keys())
        writer.writeheader()
        writer.writerows(coins)

    print(f"  ✓ Saved to {OUTPUT_CSV}")

    # Step 3 — Print preview (top 3 coins)
    print("\n  Preview (top 3 by market cap):")
    for coin in coins[:3]:
        print(f"    {coin['name']:15s}  "
              f"${coin['current_price']:>12,.2f}  "
              f"({coin.get('price_change_percentage_24h', 0):+.2f}% 24h)")

    print("\n  ✓ Task 1 complete — data ready for PySpark ETL")
    return len(coins)


if __name__ == "__main__":
    count = fetch_and_save()
    print(f"\nTotal coins saved: {count}")
    sys.exit(0)
