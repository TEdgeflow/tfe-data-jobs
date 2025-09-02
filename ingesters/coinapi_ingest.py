# ingesters/coinapi_ingest.py

import os
import time
import requests
from datetime import datetime, timezone
from supabase import create_client, Client

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
COINAPI_KEY = os.getenv("COINAPI_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY")

if not COINAPI_KEY:
    raise RuntimeError("Missing COINAPI_KEY (sign up at coinapi.io)")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE_URL = "https://rest.coinapi.io/v1/"
HEADERS = {"X-CoinAPI-Key": COINAPI_KEY}

# === Fetch Open Interest ===
def fetch_oi(symbol="BTCUSDT"):
    url = f"{BASE_URL}futures/openinterest"
    params = {"symbol_id": f"BINANCE_FUTURES_{symbol}"}
    try:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            print("⚠️ No OI data")
            return

        row = {
            "symbol": symbol,
            "oi": float(data[0].get("open_interest", 0)),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        sb.table("coinapi_oi").insert(row).execute()
        print("✅ Inserted OI:", row)

    except Exception as e:
        print("❌ OI error:", e)

# === Fetch Funding Rate ===
def fetch_funding(symbol="BTCUSDT"):
    url = f"{BASE_URL}futures/fundingrate"
    params = {"symbol_id": f"BINANCE_FUTURES_{symbol}"}
    try:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            print("⚠️ No funding data")
            return

        row = {
            "symbol": symbol,
            "funding_rate": float(data[0].get("funding_rate", 0)),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        sb.table("coinapi_funding").insert(row).execute()
        print("✅ Inserted funding:", row)

    except Exception as e:
        print("❌ Funding error:", e)


def main():
    symbols = ["BTCUSDT", "ETHUSDT"]  # extend with more later

    while True:
        for sym in symbols:
            fetch_oi(sym)
            fetch_funding(sym)
            time.sleep(2)  # small delay between calls
        time.sleep(60)  # repeat every 1 min

if __name__ == "__main__":
    main()
