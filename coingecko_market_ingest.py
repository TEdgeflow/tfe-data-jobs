import os
import time
import requests
from datetime import datetime, timezone
from supabase import create_client, Client

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

COINGECKO_API = "https://api.coingecko.com/api/v3/coins/markets"
VS_CURRENCY = "usd"

# ========= FETCH DATA =========
def fetch_coingecko_data():
    url = f"{COINGECKO_API}?vs_currency={VS_CURRENCY}&order=market_cap_desc&per_page=20&page=1&sparkline=false"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    rows = []
    ts = datetime.now(timezone.utc).isoformat()

    for d in data:
        rows.append({
            "ts": ts,
            "symbol": d.get("symbol", "").upper() + "USDT",  # unify with Binance symbols
            "price": d.get("current_price"),
            "volume_24h": d.get("total_volume"),
            "price_change_24h": d.get("price_change_percentage_24h"),
            "market_cap": d.get("market_cap")
        })

    return rows

# ========= UPSERT =========
def upsert_market_data(rows):
    if rows:
        sb.table("market_data").upsert(rows).execute()
        print(f"[upsert] {len(rows)} Coingecko market rows…")

# ========= MAIN LOOP =========
def main():
    while True:
        print("Starting Coingecko Market Data Job…")
        try:
            rows = fetch_coingecko_data()
            upsert_market_data(rows)
            print("Done.")
        except Exception as e:
            print("Error during Coingecko job:", e)

        # wait 5 minutes (300s) before next update
        time.sleep(300)

if __name__ == "__main__":
    main()

