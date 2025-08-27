import os, time, requests
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
LUNAR_API_KEY = os.getenv("LUNAR_API_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE_URL = "https://lunarcrush.com/api4/public"
HEADERS = {"Authorization": f"Bearer {LUNAR_API_KEY}"}

def fetch_categories(limit=10):
    # ✅ Correct endpoint is /category/v1 (singular)
    url = f"{BASE_URL}/category/v1"
    params = {"limit": limit, "sort": "interactions_24h", "desc": "true"}
    resp = requests.get(url, headers=HEADERS, params=params)
    resp.raise_for_status()
    return resp.json().get("data", [])

def upsert_categories(data):
    rows = []
    for c in data:
        rows.append({
            "category": c.get("category"),
            "interactions_24h": c.get("interactions_24h"),
            "num_contributors": c.get("num_contributors"),
            "category_rank": c.get("category_rank")
        })
    if rows:
        sb.table("social_categories").upsert(rows).execute()
        print(f"[✅] Upserted {len(rows)} category rows")

def main():
    while True:
        try:
            print("📊 Fetching categories...")
            cats = fetch_categories(limit=10)
            upsert_categories(cats)
        except Exception as e:
            print(f"❌ Categories error: {e}")
        time.sleep(300)

if __name__ == "__main__":
    main()

