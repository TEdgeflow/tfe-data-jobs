import os
import time
import requests
from supabase import create_client, Client
from datetime import datetime, timezone

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
LIMIT_SYMBOLS = int(os.getenv("LIMIT_SYMBOLS", "50"))  # default: top 50

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ========= Coingecko Endpoint =========
COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"


def fetch_market_data():
    """Fetch top coin data from Coingecko"""
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": LIMIT_SYMBOLS,
        "page": 1,
        "sparkline": "false"
    }
    resp = requests.get(COINGECKO_URL, params=params)
    resp.raise_for_status()
    return resp.json()


def upsert_market_data(data):
    """Insert/Update rows in Supabase"""
    rows = []
    for d in data:
        rows.append({
            "ts": datetime.now(timezone.utc).isoformat(),
            "symbol": d["symbol"].upper() + "USDT",  # e.g., BTC → BTCUSDT
            "price": d.get("current_price"),
            "volume_usd": d.get("total_volume"),
            "market_cap": d.get("market_cap")
        })

    if rows:
        sb.table("market_data").upsert(rows).execute()
        print(f"[upsert] {len(rows)} rows saved.")


def main():
    while True:
        try:
            print("🚀 Fetching Coingecko market data...")
            data = fetch_market_data()
            upsert_market_data(data)
            print("✅ Done cycle. Sleeping 5 minutes...\n")
        except Exception as e:
            print("❌ Error in Coingecko job:", e)

        time.sleep(300)  # every 5 minutes


if __name__ == "__main__":
    main()


