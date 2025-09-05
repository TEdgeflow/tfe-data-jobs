import os
import requests
from supabase import create_client, Client
from datetime import datetime

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DROPTABS_KEY = os.getenv("DROPTABS_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE_URL = "https://public-api.dropstab.com/api/v1"

def fetch_api(endpoint, page=0):
    url = f"{BASE_URL}/{endpoint}"
    params = {"api_key": DROPTABS_KEY, "pageSize": 50, "page": page}
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    data = resp.json()
    return data.get("data", {}).get("content", [])

def ingest_funding():
    print("📥 Fetching funding rounds...")
    rows = fetch_api("fundingRounds")
    for fr in rows:
        sb.table("droptabs_funding").upsert({
            "id": fr.get("id"),
            "coin_slug": fr.get("coinSlug"),
            "coin_symbol": fr.get("coinSymbol"),
            "funds_raised": fr.get("fundsRaised"),
            "pre_valuation": fr.get("preValuation"),
            "stage": fr.get("stage"),
            "category": fr.get("category"),
            "date": fr.get("date"),
            "last_update": datetime.utcnow().isoformat()
        }).execute()

def run_all():
    ingest_funding()
    print("✅ Finished Droptabs Funding ingestion.")

if __name__ == "__main__":
    run_all()


