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

# ========= Coingecko Endpoint =========
COINGECKO_URL = (
    "https://api.coingecko.com/api/v3/coins/markets"
    "?vs_currency=usd&order=market_cap_desc&per_page=50&page=1&sparkline=false"
)

def fetch_coins():
    resp = requests.get(COINGECKO_URL)
    print("[debug] status", resp.status_code)
    resp.raise_for_status()
    return resp.json()

def upsert_market(data):
    rows = []
    for d in data:
        rows.append({
            "ts": datetime.now(timezone.utc).isoformat(),
            "symbol": (d.get("symbol", "").upper() + "USDT") if d.get("symbol") else None,
            "price": d.get("current_price"),
            "volume_usd": d.get("total_volume"),
            "market_cap": d.get("market_cap"),
            "price_change_24h": d.get("price_change_percentage_24h")
        })
    if rows:
        sb.table("market_data").upsert(rows).execute()
        print(f"[upsert] {len(rows)} rows inserted/updated")

def main():
    while True:
        try:
            print("📥 Fetching Coingecko market data...")
            data = fetch_coins()
            upsert_market(data)
            print("✅ Done Coingecko batch.")
        except Exception as e:
            print("❌ Error in Coingecko job:", e)
        time.sleep(3600)  # run every 1 hour

if __name__ == "__main__":
    main()




