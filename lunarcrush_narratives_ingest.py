import os
import time
import requests
from supabase import create_client, Client

# === Supabase setup ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
LUNAR_API_KEY = os.getenv("LUNAR_API_KEY")

if not SUPABASE_URL or not SUPABASE_KEY or not LUNAR_API_KEY:
    raise RuntimeError("Missing SUPABASE_URL, SUPABASE_KEY, or LUNAR_API_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE_URL = "https://lunarcrush.com/api4/public"
HEADERS = {"Authorization": f"Bearer {LUNAR_API_KEY}"}


# === Fetch Top Mentions ===
def fetch_mentions(limit=20):
    url = f"{BASE_URL}/coins/list/v1"
    params = {"limit": limit, "sort": "social_volume_24h", "desc": "true"}
    resp = requests.get(url, headers=HEADERS, params=params)
    resp.raise_for_status()
    return resp.json().get("data", [])


# === Fetch Top Categories ===
def fetch_categories(limit=10):
    url = f"{BASE_URL}/category/cryptocurrencies/v1"
    params = {"limit": limit, "sort": "interactions_24h", "desc": "true"}
    resp = requests.get(url, headers=HEADERS, params=params)
    resp.raise_for_status()
    return resp.json().get("data", [])


# === Upsert to Supabase ===
def upsert_narratives(mentions, categories):
    rows = []

    # Mentions data
    for m in mentions:
        rows.append({
            "symbol": m.get("symbol"),
            "mentions": m.get("social_volume_24h"),
            "interactions_24h": m.get("interactions_24h"),
            "category": None,
            "category_rank": None,
            "contributors": None
        })

    # Categories data
    for c in categories:
        rows.append({
            "symbol": None,
            "mentions": None,
            "interactions_24h": c.get("interactions_24h"),
            "category": c.get("category"),
            "category_rank": c.get("category_rank"),
            "contributors": c.get("num_contributors")
        })

    if rows:
        sb.table("social_narratives").upsert(rows).execute()
        print(f"[upsert] {len(rows)} narrative rows inserted.")


# === Main Runner ===
def main():
    while True:
        try:
            print("Fetching narrative data...")
            mentions = fetch_mentions(limit=20)
            categories = fetch_categories(limit=10)

            upsert_narratives(mentions, categories)

            print("✅ Done. Sleeping 60s...")
            time.sleep(60)

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:  # Too many requests
                print("⚠️ Rate limited. Sleeping 30s...")
                time.sleep(30)
            else:
                raise
        except Exception as e:
            print("Unexpected error:", e)
            time.sleep(30)


if __name__ == "__main__":
    main()
