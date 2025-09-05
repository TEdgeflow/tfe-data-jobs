import os
import requests
from supabase import create_client, Client
from datetime import datetime

# 🔑 Environment variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DROPTABS_KEY = os.getenv("DROPTABS_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE_URL = "https://public-api.dropstab.com/api/v1"
HEADERS = {"api_key": DROPTABS_KEY}  # first attempt

def fetch_api(endpoint, params=None):
    url = f"{BASE_URL}/{endpoint}"
    params = params or {"pageSize": 50, "page": 0}

    print("📡 Requesting:", url)
    print("🔑 Headers:", HEADERS)

    resp = requests.get(url, headers=HEADERS, params=params)

    if resp.status_code == 401:  # Unauthorized
        print("⚠️ Header auth failed, retrying with query param...")
        params["api_key"] = DROPTABS_KEY
        resp = requests.get(url, params=params)

    print("📥 Response status:", resp.status_code)
    resp.raise_for_status()
    data = resp.json()
    return data.get("data", {}).get("content", [])

def ingest_funding():
    print("🚀 Fetching funding rounds...")
    rows = fetch_api("fundingRounds")

    for fr in rows:
        sb.table("droptabs_funding").upsert({
            "id": fr.get("id"),
            "coin_slug": fr.get("coinSlug"),
            "coin_symbol": fr.get("coinSymbol"),
            "funds_raised": fr.get("fundsRaised"),
            "pre_valuation": fr.get("preValuation"),
            "pre_valuation_inaccurate": fr.get("preValuationInaccurate"),
            "stage": fr.get("stage"),
            "twitter_performance": fr.get("twitterPerformance"),
            "category": fr.get("category"),
            "date": fr.get("date"),
            "last_update": datetime.utcnow().isoformat()
        }).execute()

    print(f"✅ Inserted {len(rows)} funding rounds")

def run_all():
    ingest_funding()

if __name__ == "__main__":
    run_all()


