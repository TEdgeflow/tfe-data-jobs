import os, time, requests
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
LUNAR_API_KEY = os.getenv("LUNAR_API_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE_URL = "https://lunarcrush.com/api4/public"
HEADERS = {"Authorization": f"Bearer {LUNAR_API_KEY}"}

def fetch_influencers(limit=20):
    url = f"{BASE_URL}/influencers/v1"
    params = {"limit": limit}
    resp = requests.get(url, headers=HEADERS, params=params)
    resp.raise_for_status()
    return resp.json().get("data", [])

def upsert_influencers(data):
    rows = []
    for inf in data:
        rows.append({
            "influencer_id": inf.get("influencer_id"),
            "creator_name": inf.get("creator_name"),
            "creator_followers": inf.get("creator_followers"),
            "interactions_24h": inf.get("interactions_24h")
        })
    if rows:
        sb.table("social_influencers").upsert(rows).execute()
        print(f"[✅] Upserted {len(rows)} influencers rows")

def main():
    while True:
        try:
            print("🌐 Fetching influencers...")
            infl = fetch_influencers(limit=20)
            upsert_influencers(infl)
        except Exception as e:
            print(f"❌ Influencer error: {e}")
        time.sleep(1200)

if __name__ == "__main__":
    main()

