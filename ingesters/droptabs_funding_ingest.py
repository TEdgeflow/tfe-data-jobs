import os
import requests
from supabase import create_client, Client
from datetime import datetime

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DROPTABS_KEY = os.getenv("DROPTABS_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE_URL = "https://public-api.dropstab.com/api/v1"
HEADERS = {"api_key": DROPTABS_KEY}

def fetch_api(endpoint, params=None):
    url = f"{BASE_URL}/{endpoint}"
    resp = requests.get(url, headers=HEADERS, params=params or {"pageSize": 50, "page": 0})
    resp.raise_for_status()
    data = resp.json()
    return data.get("data", {}).get("content", [])

def ingest_funding():
    print("📥 Fetching funding rounds...")
    rows = fetch_api("fundingRounds")
    for fund in rows:
        sb.table("droptabs_funding").upsert({
            "id": fund.get("id"),
            "coin_slug": fund.get("coinSlug"),
            "coin_symbol": fund.get("coinSymbol"),
            "funds_raised": fund.get("fundsRaised"),
            "pre_valuation": fund.get("preValuation"),
            "stage": fund.get("stage"),
            "category": fund.get("category"),
            "date": fund.get("date"),
            "last_update": datetime.utcnow().isoformat()
        }).execute()
    print(f"✅ Inserted {len(rows)} funding rows")

if __name__ == "__main__":
    ingest_funding()

