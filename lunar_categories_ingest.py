import os
import time
import requests
from supabase import create_client, Client

# ====== Supabase Setup ======
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("❌ Missing Supabase environment variables")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ====== LunarCrush Setup ======
LUNAR_API_KEY = os.getenv("LUNAR_API_KEY")
BASE_URL = "https://lunarcrush.com/api4/public"

HEADERS = {"Authorization": f"Bearer {LUNAR_API_KEY}"}

# ====== Fetch Categories ======
def fetch_categories(limit=10):
    url = f"{BASE_URL}/category/cryptocurrencies/v1"
    params = {
        "limit": limit,
        "sort": "interactions_24h",
        "desc": "true"
    }
    resp = requests.get(url, headers=HEADERS, params=params)
    resp.raise_for_status()
    return resp.json().get("data", [])

# ====== Upsert to Supabase ======
def upsert_categories(data):
    rows = []
    for c in data:
        rows.append({
            "category": c.get("category"),
            "category_rank": c.get("category_rank"),
            "interactions_24h": c.get("interactions_24h"),
            "contributors": c.get("num_contributors"),
        })

    if rows:
        sb.table("social_categories").upsert(rows).execute()
        print(f"[✅] Upserted {len(rows)} category rows")
    else:
        print("⚠️ No category data found.")

# ====== Main Loop ======
def main():
    while True:
        try:
            print("📊 Fetching categories...")
            data = fetch_categories(limit=10)
            upsert_categories(data)

        except requests.exceptions.HTTPError as e:
            print(f"❌ Categories error: {e}")
            print("⏳ Sleeping 5 minutes before retry...")
            time.sleep(300)
            continue

        time.sleep(1800)  # pull every 30 minutes

if __name__ == "__main__":
    main()

