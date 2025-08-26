import os
import time
import requests
from datetime import datetime, timezone
from supabase import create_client, Client

# ===== ENV VARS =====
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
LUNAR_API_KEY = os.getenv("LUNAR_API_KEY")

if not SUPABASE_URL or not SUPABASE_KEY or not LUNAR_API_KEY:
    raise RuntimeError("Missing SUPABASE_URL, SUPABASE_KEY, or LUNAR_API_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ===== LunarCrush Endpoint =====
LUNAR_BASE = "https://lunarcrush.com/api4"

def fetch_sentiment(symbol="BTC"):
    url = f"{LUNAR_BASE}/assets"
    headers = {
        "Authorization": f"Bearer {LUNAR_API_KEY}",
        "Accept": "application/json"
    }
    params = {
        "symbol": symbol,
        "interval": "day",
        "data_points": 1
    }
    resp = requests.get(url, headers=headers, params=params)
    print("[debug] URL:", resp.url)
    print("[debug] Status:", resp.status_code)
    resp.raise_for_status()
    return resp.json()

def upsert_sentiment(data, symbol="BTC"):
    rows = []
    for d in data.get("data", []):
        rows.append({
            "ts": datetime.fromtimestamp(d["time"], tz=timezone.utc).isoformat(),
            "symbol": symbol,
            "galaxy_score": d.get("galaxy_score"),
            "alt_rank": d.get("alt_rank"),
            "social_score": d.get("social_score"),
            "price": d.get("price")
        })
    if rows:
        sb.table("lunar_sentiment").upsert(rows).execute()
        print(f"[upsert] {len(rows)} sentiment rows")

def main():
    while True:
        try:
            data = fetch_sentiment("BTC")
            upsert_sentiment(data, "BTC")
        except Exception as e:
            print("Error sentiment job:", e)
        time.sleep(3600)  # run hourly

if __name__ == "__main__":
    main()

