import os
import time
import requests
from supabase import create_client, Client

# ========= ENV =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
LUNAR_API_KEY = os.getenv("LUNAR_API_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE_URL = "https://lunarcrush.com/api4/public"
HEADERS = {"Authorization": f"Bearer {LUNAR_API_KEY}"}

# Safe request with backoff
def safe_request(url, params=None):
    for attempt in range(5):
        resp = requests.get(url, headers=HEADERS, params=params)
        if resp.status_code == 429:
            wait = 10 * (attempt + 1)
            print(f"[rate-limit] Sleeping {wait}s...")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError("Failed after 5 retries")

# Fetch top coins
def fetch_top_symbols(limit=20):
    url = f"{BASE_URL}/coins/list/v1"
    params = {"limit": limit, "sort": "market_cap_rank", "desc": True}
    data = safe_request(url, params)
    return [item["symbol"] for item in data.get("data", [])]

# Fetch sentiment for a coin
def fetch_sentiment(symbol):
    url = f"{BASE_URL}/coins/list/v1"
    params = {"symbol": symbol, "limit": 1}
    data = safe_request(url, params)
    return data["data"][0]

# Insert into Supabase
def upsert_sentiment(row, symbol):
    record = {
        "symbol": symbol,
        "galaxy_score": row.get("galaxy_score"),
        "alt_rank": row.get("alt_rank"),
        "social_volume": row.get("social_volume_24h"),
        "social_score": row.get("social_score"),
        "url_shares": row.get("url_shares", 0),
        "sentiment": row.get("sentiment", 0)
    }
    sb.table("social_sentiment").upsert(record).execute()
    print(f"[upsert] {symbol}: {record}")

def main():
    print("🚀 Starting LunarCrush Sentiment ingestion...")
    symbols = fetch_top_symbols(limit=20)
    print(f"[symbols] {symbols}")
    for sym in symbols:
        try:
            row = fetch_sentiment(sym)
            upsert_sentiment(row, sym)
            time.sleep(3)  # spread requests to avoid 429
        except Exception as e:
            print(f"[error] {sym}: {e}")
            continue
    print("✅ Done sentiment job.")

if __name__ == "__main__":
    main()

 
