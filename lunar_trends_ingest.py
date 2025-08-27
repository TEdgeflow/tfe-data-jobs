import os, time, requests
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
LUNAR_API_KEY = os.getenv("LUNAR_API_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE_URL = "https://lunarcrush.com/api4/public"
HEADERS = {"Authorization": f"Bearer {LUNAR_API_KEY}"}

def fetch_trends(symbol="LINK", interval="6m"):
    url = f"{BASE_URL}/influencer/twitter/{symbol}/v1"
    params = {"interval": interval, "bucket": "day"}
    resp = requests.get(url, headers=HEADERS, params=params)
    resp.raise_for_status()
    return resp.json().get("data", [])

def upsert_trends(data, symbol):
    rows = []
    for t in data:
        rows.append({
            "symbol": symbol,
            "followers": t.get("followers"),
            "interactions": t.get("interactions"),
            "posts_active": t.get("posts_active")
        })
    if rows:
        sb.table("social_trends").upsert(rows).execute()
        print(f"[✅] Upserted {len(rows)} trend rows for {symbol}")

def main():
    while True:
        try:
            print("📈 Fetching trends...")
            data = fetch_trends(symbol="LINK")
            upsert_trends(data, "LINK")
        except Exception as e:
            print(f"❌ Trends error: {e}")
        time.sleep(900)

if __name__ == "__main__":
    main()

