import os
import time
import requests
from supabase import create_client, Client
from datetime import datetime, timezone

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Top coins to track (you can expand this list)
COINS = [
    "bitcoin", "ethereum", "binancecoin", "ripple", "solana",
    "cardano", "dogecoin", "tron", "polkadot", "matic-network",
    "litecoin", "avalanche-2", "chainlink", "uniswap", "stellar",
    "monero", "okb", "internet-computer", "aptos", "near"
]

def fetch_coingecko_data():
    """Fetch market data from Coingecko for selected coins"""
    url = (
        "https://api.coingecko.com/api/v3/coins/markets"
        "?vs_currency=usd&ids=" + ",".join(COINS)
    )
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()

    rows = []
    ts = datetime.now(timezone.utc).isoformat()

    for coin in data:
        rows.append({
            "ts": ts,
            "symbol": coin["symbol"].upper() + "USDT",   # keep USDT format
            "price": coin["current_price"],
            "volume_24h": coin["total_volume"],
            "market_cap": coin["market_cap"],
            "price_change_24h": coin["price_change_percentage_24h"]
        })
    return rows

def upsert_market_data(rows):
    """Insert/Upsert rows into Supabase market_data"""
    sb.table("market_data").upsert(rows).execute()

if __name__ == "__main__":
    while True:
        try:
            print("Starting Coingecko Market Data Job…")
            rows = fetch_coingecko_data()
            upsert_market_data(rows)
            print(f"[upsert] {len(rows)} Coingecko market rows…")
            print("Done.")
        except Exception as e:
            print("Error during Coingecko job:", e)

        # wait 5 minutes before next update
        time.sleep(300)
