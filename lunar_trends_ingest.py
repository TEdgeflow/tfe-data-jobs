import os, time, requests
from supabase import create_client, Client
from datetime import datetime, timezone

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
LUNAR_TRENDS_API_KEY = os.getenv("LUNAR_TRENDS_API_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE_URL = "https://lunarcrush.com/api4/public/trending"

def fetch_trends(symbol):
    headers = {"Authorization": f"Bearer {LUNAR_TRENDS_API_KEY}"}
    params = {"symbol": symbol, "limit": 1}
    resp = requests.get(BASE_URL, headers=headers, params=params)
    resp.raise_for_status()
    return resp.json()

def upsert_trends(data, symbol):
    rows = []
    for item in data.get("data", []):
        rows.append({
            "ts": datetime.now(timezone.utc).isoformat(),
            "symbol": symbol,
            "followers": item.get("followers"),
            "engagement": item.get("engagement"),
            "activity_score": item.get("activityScore"),
            "change_24h": item.get("change24h"),
            "change_7d": item.get("change7d"),
        })
    if rows:
        sb.table("social_trends").upsert(rows).execute()
        print(f"[upsert] {len(rows)} trends rows for {symbol}")

def main():
    symbols = ["BTC", "ETH", "SOL", "DOGE", "MATIC"]  # expand as needed
    while True:
        for sym in symbols:
            try:
                data = fetch_trends(sym)
                upsert_trends(data, sym)
            except Exception as e:
                print("Error trends job:", e)
        time.sleep(3600)  # every 1 hour

if __name__ == "__main__":
    main()
