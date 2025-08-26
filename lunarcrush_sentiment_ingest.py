import os
import time
import requests
from supabase import create_client, Client
from datetime import datetime, timezone

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
LUNAR_API_KEY = os.getenv("LUNAR_API_KEY")

if not SUPABASE_URL or not SUPABASE_KEY or not LUNAR_API_KEY:
    raise RuntimeError("Missing one of SUPABASE_URL, SUPABASE_KEY, or LUNAR_API_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ========= LunarCrush Endpoint =========
LUNAR_BASE = "https://lunarcrush.com/api4/v1"

def fetch_sentiment(symbol="BTC"):
    url = f"{LUNAR_BASE}/assets"
    params = {
        "symbol": symbol,
        "data_points": 1,
        "interval": "day",
        "key": LUNAR_API_KEY
    }
    resp = requests.get(url, params=params)
    print("[debug] URL:", resp.url)
    print("[debug] Status:", resp.status_code)
    resp.raise_for_status()
    return resp.json()

def upsert_sentiment(data, symbol="BTC"):
    rows = []
    for d in data.get("data", []):
        rows.append({
            "ts": datetime.now(timezone.utc).isoformat(),
            "symbol": symbol,
            "galaxy_score": d.get("galaxy_score"),
            "alt_rank": d.get("alt_rank"),
            "social_volume": d.get("social_volume"),
            "social_score": d.get("social_score"),
            "url_shares": d.get("url_shares"),
            "sentiment": d.get("sentiment"),
        })
    if rows:
        sb.table("social_sentiment").upsert(rows).execute()
        print(f"[upsert] {len(rows)} sentiment rows")

def main():
    symbols = ["BTC", "ETH", "SOL", "XRP"]  # expand later
    while True:
        try:
            for sym in symbols:
                data = fetch_sentiment(sym)
                upsert_sentiment(data, symbol=sym)
            print("✅ Done social sentiment cycle.")
        except Exception as e:
            print("❌ Error sentiment job:", e)
        time.sleep(3600)  # run every 1 hour

if __name__ == "__main__":
    main()


