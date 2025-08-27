import os
import time
import requests
from supabase import create_client, Client

# ===== Supabase Setup =====
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
LUNAR_API_KEY = os.getenv("LUNAR_API_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE_URL = "https://lunarcrush.com/api4/public"
HEADERS = {"Authorization": f"Bearer {LUNAR_API_KEY}"}


# ===== API Fetchers =====
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
    url = f"{BASE_URL}/categories/v1"
    params = {"limit": limit, "sort": "interactions_24h", "desc": "true"}
    resp = requests.get(url, headers=HEADERS, params=params)
    if resp.status_code == 429:
        print("⚠️ Rate limited on categories. Sleeping 30s...")
        time.sleep(30)
        return fetch_categories(limit)
    resp.raise_for_status()
    return resp.json().get("data", [])


# ===== DB Upsert =====
def upsert_narratives(mentions, categories):
    rows = []

    # Mentions data
    for m in mentions:
        rows.append({
            "symbol": m.get("symbol"),
            "mentions": m.get("interactions_24h"),
            "interactions_24h": m.get("interactions_24h"),
            "category": "token",
            "category_rank": None,
            "contributors": m.get("contributors")
        })

    # Categories data
    for c in categories:
        rows.append({
            "symbol": None,
            "mentions": None,
            "interactions_24h": c.get("interactions_24h"),
            "category": c.get("category"),
            "category_rank": c.get("category_rank"),
            "contributors": c.get("contributors")
        })

    if rows:
        sb.table("social_narratives").upsert(rows).execute()
        print(f"[upsert] {len(rows)} rows inserted/updated.")


# ===== Main Loop =====
def main():
    while True:
        try:
            print("🔍 Fetching narrative data...")
            mentions = fetch_mentions(limit=20)
            categories = fetch_categories(limit=10)

            upsert_narratives(mentions, categories)

        except Exception as e:
            print(f"❌ Error: {e}")
            time.sleep(60)

        # Sleep between cycles
        print("⏳ Sleeping 5 minutes before next pull...")
        time.sleep(300)


if __name__ == "__main__":
    main()

