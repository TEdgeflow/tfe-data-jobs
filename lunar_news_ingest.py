import os, time, requests
from supabase import create_client, Client
from datetime import datetime, timezone

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
LUNAR_NEWS_API_KEY = os.getenv("LUNAR_NEWS_API_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE_URL = "https://lunarcrush.com/api4/public/news"

def fetch_news(symbol):
    headers = {"Authorization": f"Bearer {LUNAR_NEWS_API_KEY}"}
    params = {"symbol": symbol, "limit": 5}
    resp = requests.get(BASE_URL, headers=headers, params=params)
    resp.raise_for_status()
    return resp.json()

def upsert_news(data, symbol):
    rows = []
    for item in data.get("data", []):
        rows.append({
            "ts": datetime.now(timezone.utc).isoformat(),
            "symbol": symbol,
            "title": item.get("title"),
            "url": item.get("url"),
            "source": item.get("source"),
            "sentiment_score": item.get("sentiment", {}).get("score"),
            "interactions": item.get("interactionsCount"),
            "published_at": item.get("publishedAt")
        })
    if rows:
        sb.table("social_news").upsert(rows).execute()
        print(f"[upsert] {len(rows)} news rows for {symbol}")

def main():
    symbols = ["BTC", "ETH", "SOL", "AVAX", "XRP"]  # expand as needed
    while True:
        for sym in symbols:
            try:
                data = fetch_news(sym)
                upsert_news(data, sym)
            except Exception as e:
                print("Error news job:", e)
        time.sleep(1800)  # every 30 minutes

if __name__ == "__main__":
    main()
