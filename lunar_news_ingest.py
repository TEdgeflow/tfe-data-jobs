import os, time, requests
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
LUNAR_API_KEY = os.getenv("LUNAR_API_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE_URL = "https://lunarcrush.com/api4/public"
HEADERS = {"Authorization": f"Bearer {LUNAR_API_KEY}"}

def fetch_news(limit=20):
    url = f"{BASE_URL}/category/cryptocurrencies/news/v1"
    params = {"limit": limit}
    resp = requests.get(url, headers=HEADERS, params=params)
    resp.raise_for_status()
    return resp.json().get("data", [])

def upsert_news(data):
    rows = []
    for n in data:
        rows.append({
            "post_title": n.get("post_title"),
            "post_link": n.get("post_link"),
            "post_sentiment": n.get("post_sentiment"),
            "creator_name": n.get("creator_name"),
            "interactions_total": n.get("interactions_total")
        })
    if rows:
        sb.table("social_news").upsert(rows).execute()
        print(f"[✅] Upserted {len(rows)} news rows")

def main():
    while True:
        try:
            print("📰 Fetching news...")
            news = fetch_news(limit=20)
            upsert_news(news)
        except Exception as e:
            print(f"❌ News error: {e}")
        time.sleep(600)

if __name__ == "__main__":
    main()

