import os, time, requests
from datetime import datetime, timezone
from supabase import create_client, Client

# ====== ENV ======
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
LUNAR_API_KEY = os.getenv("LUNAR_API_KEY")

if not SUPABASE_URL or not SUPABASE_KEY or not LUNAR_API_KEY:
    raise RuntimeError("Missing one of SUPABASE_URL, SUPABASE_KEY, LUNAR_API_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ====== LunarCrush Endpoint ======
LUNAR_BASE = "https://lunarcrush.com/api3"

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
    try:
        metrics = data.get("data", [])[0]  # first element
        rows.append({
            "ts": datetime.now(timezone.utc).isoformat(),
            "symbol": symbol,
            "galaxy_score": metrics.get("galaxy_score"),
            "alt_rank": metrics.get("alt_rank"),
            "social_volume": metrics.get("social_volume"),
            "social_score": metrics.get("social_score"),
        })
    except Exception as e:
        print("[error] parsing sentiment:", e)
    if rows:
        sb.table("lunar_sentiment").upsert(rows).execute()
        print(f"[upsert] {len(rows)} sentiment rows")

def main():
    while True:
        try:
            data = fetch_sentiment("BTC")
            upsert_sentiment(data, "BTC")
            print("Done sentiment.")
        except Exception as e:
            print("Error sentiment job:", e)
        time.sleep(3600)  # run hourly

if __name__ == "__main__":
    main()

