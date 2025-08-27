import os
import time
import requests
from supabase import create_client, Client

# ===== Config =====
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
LUNAR_API_KEY = os.getenv("LUNAR_API_KEY")

HEADERS = {"Authorization": f"Bearer {LUNAR_API_KEY}"}
BASE_URL = "https://lunarcrush.com/api4/public"

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ===== Fetch Functions =====
def fetch_mentions(limit=20):
    """Fetch top mentioned coins"""
    url = f"{BASE_URL}/coins/list/v1"
    params = {"limit": limit, "sort": "interactions_24h", "desc": "true"}
    resp = requests.get(url, headers=HEADERS, params=params)
    if resp.status_code == 429:
        print("⚠️ Rate limited on mentions. Sleeping 30s...")
        time.sleep(30)
        return fetch_mentions(limit)
    resp.raise_for_status()
    return resp.json().get("data", [])

def fetch_categories(limit=10):
    """Fetch top categories by interactions"""
    url = f"{BASE_URL}/category/cryptocurrencies/v1"
    params = {"limit": limit, "sort": "interactions_24h", "desc": "true"}
    resp = requests.get(url, headers=HEADERS, params=params)
    if resp.status_code == 429:
        print("⚠️ Rate limited on categories. Sleeping 30s...")
        time.sleep(30)
        return fetch_categories(limit)
    resp.raise_for_status()
    return resp.json().get("data", [])

# ===== Upsert Functions =====
def upsert_mentions(data):
    rows = []
    for d in data:
        rows.append({
            "symbol": d.get("symbol"),
            "mentions": d.get("interactions_24h"),
            "interactions_24h": d.get("interactions_24h"),
            "category": None,
            "category_rank": None,
            "contributors": None
        })
    if rows:
        sb.table("social_narratives").upsert(rows).execute()
        print(f"[✅] Upserted {len(rows)} mentions rows")

def upsert_categories(data):
    rows = []
    for d in data:
        rows.append({
            "symbol": None,
            "mentions": None,
            "interactions_24h": d.get("interactions_24h"),
            "category": d.get("category"),
            "category_rank": d.get("category_rank"),
            "contributors": d.get("num_contributors")
        })
    if rows:
        sb.table("social_narratives").upsert(rows).execute()
        print(f"[✅] Upserted {len(rows)} category rows")

# ===== Main =====
def main():
    print("🔍 Fetching narrative data...")

    try:
        mentions = fetch_mentions(limit=20)
        upsert_mentions(mentions)
    except Exception as e:
        print("❌ Mentions error:", e)

    try:
        categories = fetch_categories(limit=10)
        upsert_categories(categories)
    except Exception as e:
        print("❌ Categories error:", e)

if __name__ == "__main__":
    while True:
        main()
        print("⏳ Sleeping 5 minutes before next pull...")
        time.sleep(300)


