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
    raise RuntimeError("Missing SUPABASE or LUNARCRUSH vars")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

LUNAR_API = "https://lunarcrush.com/api3/coins"

def fetch_sentiment(symbol):
    """Fetch sentiment metrics from LunarCrush"""
    headers = {"Authorization": f"Bearer {LUNAR_API_KEY}"}
    resp = requests.get(f"{LUNAR_API}?symbol={symbol}", headers=headers)
    resp.raise_for_status()
    data = resp.json()
    if "data" not in data or not data["data"]:
        return None
    return data["data"][0]  # first coin

def upsert_sentiment(symbol, data):
    row = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "symbol": symbol,
        "galaxy_score": data.get("galaxy_score"),
        "alt_rank": data.get("alt_rank"),
        "social_volume": data.get("social_volume"),
        "social_score": data.get("social_score"),
        "url_shares": data.get("url_shares"),
        "sentiment": data.get("sentiment")
    }
    sb.table("social_sentiment").upsert(row).execute()
    print(f"[upsert] {symbol} sentiment row inserted")

def main():
    symbols = ["BTC", "ETH", "SOL", "BNB", "AVAX"]  # start with a handful
    while True:
        for sym in symbols:
            try:
                data = fetch_sentiment(sym)
                if data:
                    upsert_sentiment(sym, data)
            except Exception as e:
                print(f"[error] {sym}:", e)
        time.sleep(600)  # every 10 min

if __name__ == "__main__":
    main()
