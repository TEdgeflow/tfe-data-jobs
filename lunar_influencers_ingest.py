import os, time, requests
from supabase import create_client, Client
from datetime import datetime, timezone

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
LUNAR_INFLUENCERS_API_KEY = os.getenv("LUNAR_INFLUENCERS_API_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE_URL = "https://lunarcrush.com/api4/public/influencers"

def fetch_influencers():
    headers = {"Authorization": f"Bearer {LUNAR_INFLUENCERS_API_KEY}"}
    params = {"limit": 20}
    resp = requests.get(BASE_URL, headers=headers, params=params)
    resp.raise_for_status()
    return resp.json()

def upsert_influencers(data):
    rows = []
    for item in data.get("data", []):
        rows.append({
            "ts": datetime.now(timezone.utc).isoformat(),
            "handle": item.get("handle"),
            "platform": item.get("platform"),
            "followers": item.get("followers"),
            "engagement_rate": item.get("engagementRate"),
            "influence_score": item.get("influenceScore"),
            "mentions": item.get("mentions")
        })
    if rows:
        sb.table("social_influencers").upsert(rows).execute()
        print(f"[upsert] {len(rows)} influencers rows")

def main():
    while True:
        try:
            data = fetch_influencers()
            upsert_influencers(data)
        except Exception as e:
            print("Error influencers job:", e)
        time.sleep(7200)  # every 2 hours

if __name__ == "__main__":
    main()
