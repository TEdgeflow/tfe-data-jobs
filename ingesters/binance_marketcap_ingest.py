import os
import requests
from datetime import datetime, timezone
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_binance_market_data():
    url = "https://api.binance.com/api/v3/ticker/24hr"
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    data = response.json()

    rows = []
    ts = datetime.now(timezone.utc).isoformat()

    for d in data:
        if not d["symbol"].endswith("USDT"):
            continue
        price = float(d["lastPrice"])
        volume = float(d["quoteVolume"])
        rows.append({
            "ts": ts,
            "symbol": d["symbol"],
            "price": price,
            "volume_24h": volume,
            "market_cap_est": price * volume,  # proxy market cap
            "updated_at": ts,
        })

    if rows:
        sb.table("binance_market_cap").upsert(rows, on_conflict=["ts", "symbol"]).execute()
        print(f"[INFO] Inserted {len(rows)} market cap rows at {ts}")

if __name__ == "__main__":
    fetch_binance_market_data()
