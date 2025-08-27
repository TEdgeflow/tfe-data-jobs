import os
import time
import requests
from supabase import create_client, Client

# ==============================
# Environment Variables
# ==============================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
LUNAR_API_KEY = os.getenv("LUNAR_API_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY in environment variables")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE_URL = "https://lunarcrush.com/api4/public"
HEADERS = {"Authorization": f"Bearer {LUNAR_API_KEY}"}


# ==============================
# Fetch Mentions (coins/list)
# ==============================
def fetch_mentions(limit=20):
    url = f"{BASE_URL}/coins/list/v1"
    params = {
        "limit": limit,
        "sort": "social_volume_24h",
        "desc": "true"
    }
    resp = requests.get(url, headers=HEADERS, params=params)
    resp.raise_for_status()
    return resp.json().get("data", [])


# ==============================
# Upsert into Supabase
# ==============================
def upsert_mentions(data):
    rows = []
    for coin in data:
        rows.append({
            "symbol": coin.get("symbol"),
            "social_volume_24h": coin.get("social_volume_24h"),
            "interactions_24h": coin.get("interactions_24h"),
            "unique_social_contributors_24h": coin.get("unique_social_contributors_24h")
        })

    if rows:
        res = sb.table("social_mentions").upsert(rows).execute()
        print(f"[✅] Upserted {len(rows)} mention rows")
    else:
        print("[⚠️] No mention rows to insert")


# ==============================
# Main Loop (rate limited)
# ==============================
def main():
    while True:
        try:
            print("🔍 Fetching mentions...")
            data = fetch_mentions(limit=20)
            upsert_mentions(data)

        except requests.exceptions.HTTPError as e:
            print(f"❌ HTTP error: {e}")
            if e.response.status_code == 429:
                print("⏳ Rate limit hit. Sleeping 60s...")
                time.sleep(60)
            else:
                time.sleep(15)

        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            time.sleep(30)

        # Sleep 5 minutes between pulls
        time.sleep(300)


if __name__ == "__main__":
    print("🚀 Starting LunarCrush Mentions Ingestion...")
    main()


