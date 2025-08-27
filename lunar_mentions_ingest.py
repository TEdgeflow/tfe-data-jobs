import os
import time
import requests
from supabase import create_client, Client

# ===== Env Vars =====
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
LUNAR_API_KEY = os.getenv("LUNAR_API_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("❌ Missing SUPABASE_URL or SUPABASE_KEY in environment")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ===== LunarCrush Endpoint =====
BASE_URL = "https://lunarcrush.com/api4/public"
HEADERS = {"Authorization": f"Bearer {LUNAR_API_KEY}"}


# --- Fetch Mentions Data ---
def fetch_mentions(limit=20):
    url = f"{BASE_URL}/coins/list/v1"
    params = {
        "limit": limit,
        "sort": "interactions_24h",
        "desc": "true"
    }
    resp = requests.get(url, headers=HEADERS, params=params)
    resp.raise_for_status()
    return resp.json().get("data", [])


# --- Upsert into Supabase ---
def upsert_mentions(data):
    rows = []
    for m in data:
        rows.append({
            "symbol": m.get("symbol"),
            "social_volume_24h": m.get("social_volume_24h"),
            "interactions_24h": m.get("interactions_24h"),
            "contributors": m.get("contributors")
        })

    if rows:
        sb.table("social_mentions").upsert(rows).execute()
        print(f"[✅] Upserted {len(rows)} mentions rows")
    else:
        print("⚠️ No mentions data returned")


# --- Main Loop ---
def main():
    while True:
        print("🔍 Fetching mentions...")
        try:
            mentions_data = fetch_mentions(limit=20)
            upsert_mentions(mentions_data)

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                print("⚠️ Rate limited. Sleeping 60s...")
                time.sleep(60)
                continue
            else:
                print(f"❌ Mentions error: {e}")
        except Exception as e:
            print(f"❌ Unexpected error: {e}")

        # Sleep before next pull
        time.sleep(300)  # every 5 minutes


if __name__ == "__main__":
    print("🚀 Starting LunarCrush Mentions Ingestion...")
    main()
