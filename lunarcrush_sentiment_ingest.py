import os
import requests
from supabase import create_client, Client
from datetime import datetime, timezone

# ===== Supabase Setup =====
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
LUNAR_API_KEY = os.getenv("LUNAR_API_KEY")

if not SUPABASE_URL or not SUPABASE_KEY or not LUNAR_API_KEY:
    raise RuntimeError("Missing SUPABASE_URL, SUPABASE_KEY, or LUNAR_API_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

HEADERS = {"Authorization": f"Bearer {LUNAR_API_KEY}"}
BASE_URL = "https://lunarcrush.com/api4/public"

# ===== Fetch Functions =====

def fetch_sentiment(symbol="BTC"):
    url = f"{BASE_URL}/coins/list/v1"
    params = {"symbol": symbol, "limit": 1}
    resp = requests.get(url, headers=HEADERS, params=params)
    resp.raise_for_status()
    return resp.json().get("data", [])

def fetch_news():
    url = f"{BASE_URL}/category/cryptocurrencies/news/v1"
    params = {"limit": 10}
    resp = requests.get(url, headers=HEADERS, params=params)
    resp.raise_for_status()
    return resp.json().get("data", [])

def fetch_trends(symbol="BTC"):
    url = f"{BASE_URL}/influencers/list/v1"
    params = {"symbol": symbol, "limit": 5}
    resp = requests.get(url, headers=HEADERS, params=params)
    resp.raise_for_status()
    return resp.json().get("data", [])

# ===== Upsert Functions =====

def upsert_sentiment(data):
    rows = []
    for d in data:
        rows.append({
            "symbol": d.get("symbol"),
            "galaxy_score": d.get("galaxy_score"),
            "alt_rank": d.get("alt_rank"),
            "social_volume": d.get("social_volume_24h"),
            "social_score": d.get("average_sentiment"),  # mapped to social_score
            "url_shares": d.get("url_shares"),
            "sentiment": d.get("sentiment")
        })
    if rows:
        sb.table("social_sentiment").upsert(rows).execute()
        print(f"[upsert] {len(rows)} sentiment rows")

def upsert_news(data):
    rows = []
    for d in data:
        rows.append({
            "symbol": "crypto",  # news may not always have symbol
            "post_title": d.get("post_title"),
            "post_link": d.get("post_link"),
            "post_type": d.get("post_type"),
            "source": d.get("creator_name"),
            "post_sentiment": d.get("post_sentiment"),
            "interactions": d.get("interactions_total")
        })
    if rows:
        sb.table("social_news").upsert(rows).execute()
        print(f"[upsert] {len(rows)} news rows")

def upsert_trends(data, symbol="BTC"):
    rows = []
    for d in data:
        rows.append({
            "symbol": symbol,
            "influencer": d.get("influencer_name"),
            "followers": d.get("followers"),
            "interactions": d.get("interactions_total"),
            "posts_active": d.get("posts_active"),
            "posts_created": d.get("posts_created"),
            "follower_change": d.get("followers_change")
        })
    if rows:
        sb.table("social_trends").upsert(rows).execute()
        print(f"[upsert] {len(rows)} trend rows")

# ===== Main Runner =====

def main():
    print("Starting LunarCrush Ingestion...")

    # Sentiment
    sentiment_data = fetch_sentiment("BTC")
    upsert_sentiment(sentiment_data)

    # News
    news_data = fetch_news()
    upsert_news(news_data)

    # Trends
    trends_data = fetch_trends("BTC")
    upsert_trends(trends_data, symbol="BTC")

    print("✅ Done with LunarCrush ingestion.")

if __name__ == "__main__":
    main()

 
