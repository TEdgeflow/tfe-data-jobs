import os
import requests
from supabase import create_client, Client

# ===== ENV VARS =====
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
LUNAR_API_KEY = os.getenv("LUNAR_API_KEY")

if not SUPABASE_URL or not SUPABASE_KEY or not LUNAR_API_KEY:
    raise RuntimeError("Missing SUPABASE or LUNAR API credentials")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

LUNAR_BASE = "https://lunarcrush.com/api4/public"

HEADERS = {"Authorization": f"Bearer {LUNAR_API_KEY}"}


# === Sentiment (Galaxy Score etc.) ===
def fetch_sentiment(symbol="BTC"):
    url = f"{LUNAR_BASE}/coins/list/v1"
    params = {"symbol": symbol}
    r = requests.get(url, headers=HEADERS, params=params)
    r.raise_for_status()
    return r.json()


def upsert_sentiment(data, symbol="BTC"):
    if "data" not in data:
        return
    d = data["data"][0]
    row = {
        "symbol": symbol,
        "galaxy_score": d.get("galaxy_score"),
        "alt_rank": d.get("alt_rank"),
        "social_volume": d.get("social_volume_24h"),
        "social_score": d.get("social_score"),
    }
    sb.table("social_sentiment").insert(row).execute()
    print(f"[upsert] sentiment for {symbol}")


# === News/Posts ===
def fetch_news():
    url = f"{LUNAR_BASE}/category/cryptocurrencies/news/v1"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    return r.json()


def upsert_news(data):
    if "data" not in data:
        return
    rows = []
    for d in data["data"]:
        row = {
            "symbol": "crypto",  # could map later
            "post_title": d.get("post_title"),
            "post_link": d.get("post_link"),
            "post_type": d.get("post_type"),
            "source": d.get("creator_name"),
            "post_sentiment": d.get("post_sentiment"),
            "interactions": d.get("interactions_total"),
        }
        rows.append(row)
    if rows:
        sb.table("social_news").insert(rows).execute()
        print(f"[upsert] {len(rows)} news rows")


# === Social Trends (follower growth etc.) ===
def fetch_trends(influencer="chainlink"):
    url = f"{LUNAR_BASE}/influencer/trends/v1"
    params = {"network": "twitter", "influencer_id": influencer}
    r = requests.get(url, headers=HEADERS, params=params)
    r.raise_for_status()
    return r.json()


def upsert_trends(data, symbol="LINK"):
    if "data" not in data:
        return
    rows = []
    for d in data["data"]:
        row = {
            "symbol": symbol,
            "influencer": "chainlink",
            "followers": d.get("followers"),
            "interactions": d.get("interactions"),
            "posts_active": d.get("posts_active"),
            "posts_created": d.get("posts_created"),
            "follower_change": d.get("change"),
        }
        rows.append(row)
    if rows:
        sb.table("social_trends").insert(rows).execute()
        print(f"[upsert] {len(rows)} trend rows")


# === MAIN JOB ===
def run_job():
    print("Starting LunarCrush sentiment job...")

    # Sentiment (per symbol)
    sentiment = fetch_sentiment("BTC")
    upsert_sentiment(sentiment, "BTC")

    # News
    news = fetch_news()
    upsert_news(news)

    # Trends
    trends = fetch_trends("chainlink")
    upsert_trends(trends, "LINK")

    print("Done LunarCrush job.")


if __name__ == "__main__":
    run_job()
