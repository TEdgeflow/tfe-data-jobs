import os
import time
import requests
from supabase import create_client, Client

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
LUNAR_API_KEY = os.getenv("LUNAR_API_KEY")

if not SUPABASE_URL or not SUPABASE_KEY or not LUNAR_API_KEY:
    raise RuntimeError("Missing one of SUPABASE_URL, SUPABASE_KEY, LUNAR_API_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE_URL = "https://lunarcrush.com/api4/public"
HEADERS = {"Authorization": f"Bearer {LUNAR_API_KEY}"}

# ========= FETCH FUNCTIONS =========

def fetch_top_symbols(limit=20):
    """Fetch top mentioned coins (default top 20)."""
    url = f"{BASE_URL}/coins/list/v1"
    params = {"limit": limit, "sort": "market_cap_rank", "desc": True}
    resp = requests.get(url, headers=HEADERS, params=params)
    resp.raise_for_status()
    data = resp.json()
    return [item["symbol"] for item in data.get("data", [])]


def fetch_sentiment(symbol="BTC"):
    """Fetch sentiment metrics for a given symbol."""
    url = f"{BASE_URL}/coins/list/v1"
    params = {"symbol": symbol, "limit": 1}
    resp = requests.get(url, headers=HEADERS, params=params)
    
    # Rate-limit handling
    if resp.status_code == 429:
        print(f"[rate-limit] Sleeping 10s before retry for {symbol}...")
        time.sleep(10)
        resp = requests.get(url, headers=HEADERS, params=params)

    resp.raise_for_status()
    data = resp.json()
    if "data" not in data or not data["data"]:
        raise ValueError(f"No sentiment data returned for {symbol}")
    return data["data"][0]


# ========= UPSERT FUNCTION =========

def upsert_sentiment(row, symbol):
    """Upsert row into Supabase social_sentiment table."""
    record = {
        "symbol": symbol,
        "galaxy_score": row.get("galaxy_score"),
        "alt_rank": row.get("alt_rank"),
        "social_volume": row.get("social_volume_24h"),
        "social_score": row.get("social_score"),
        "url_shares": row.get("url_shares", 0),
        "sentiment": row.get("sentiment", 0)
    }
    print(f"[upsert] {symbol}: {record}")
    sb.table("social_sentiment").upsert(record).execute()


# ========= MAIN LOOP =========

def main():
    print("Starting LunarCrush Sentiment Ingestion...")

    try:
        symbols = fetch_top_symbols(limit=20)
        print(f"[symbols] Found {len(symbols)} top symbols: {symbols}")
    except Exception as e:
        print("[error] Could not fetch top symbols:", e)
        return

    for sym in symbols:
        try:
            data = fetch_sentiment(sym)
            upsert_sentiment(data, sym)
            time.sleep(3)  # prevent hitting 429
        except Exception as e:
            print(f"[error] {sym}: {e}")
            continue

    print("Done LunarCrush ingestion ✅")


if __name__ == "__main__":
    main()

 
