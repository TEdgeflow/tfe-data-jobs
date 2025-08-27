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

def fetch_mentions(limit=20):
    """Fetch top mentioned coins"""
    url = f"{BASE_URL}/mentions/v1?limit={limit}&sort=interactions_24h&desc=true"
    headers = {"Authorization": f"Bearer {LUNAR_API_KEY}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()

def upsert_mentions(data):
    rows = []
    for item in data.get("data", []):
        rows.append({
            "ts": "now()",
            "symbol": item.get("symbol"),
            "mentions": item.get("interactions_24h"),
        })
    if rows:
        sb.table("social_mentions").upsert(rows).execute()
        print(f"[✅] Upserted {len(rows)} mentions rows")

def main():
    while True:
        try:
            print("🔍 Fetching mentions...")
            data = fetch_mentions(limit=20)
            upsert_mentions(data)
        except requests.exceptions.HTTPError as e:
            print(f"❌ Mentions error: {e}")
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
        time.sleep(300)  # run every 5 min

if __name__ == "__main__":
    main()

