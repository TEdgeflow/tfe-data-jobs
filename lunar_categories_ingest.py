import os
import time
import requests
from supabase import create_client, Client

# Env Vars
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
LUNAR_API_KEY = os.getenv("LUNAR_API_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Missing SUPABASE_URL or SUPABASE_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE_URL = "https://lunarcrush.com/api4/public"

def fetch_categories(limit=10):
    """Fetch top social categories"""
    url = f"{BASE_URL}/categories/v1?limit={limit}&sort=interactions_24h&desc=true"
    headers = {"Authorization": f"Bearer {LUNAR_API_KEY}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()

def upsert_categories(data):
    rows = []
    for item in data.get("data", []):
        rows.append({
            "ts": "now()",
            "category": item.get("category"),
            "interactions_24h": item.get("interactions_24h"),
            "category_rank": item.get("category_rank"),
            "contributors": item.get("num_contributors"),
        })
    if rows:
        sb.table("social_categories").upsert(rows).execute()
        print(f"[✅] Upserted {len(rows)} categories rows")

def main():
    while True:
        try:
            print("🔍 Fetching categories...")
            data = fetch_categories(limit=10)
            upsert_categories(data)
        except requests.exceptions.HTTPError as e:
            print(f"❌ Categories error: {e}")
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
        time.sleep(600)  # run every 10 min

if __name__ == "__main__":
    main()
