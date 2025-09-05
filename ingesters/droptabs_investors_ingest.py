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

def ingest_investors():
    print("📥 Fetching investors...")
    rows = fetch_api("investors")
    for inv in rows:
        sb.table("droptabs_investors").upsert({
            "id": inv.get("id"),
            "investor_slug": inv.get("investorSlug"),
            "name": inv.get("name"),
            "country": inv.get("country"),
            "tier": inv.get("tier"),
            "venture_type": inv.get("ventureType"),
            "rank": inv.get("rank"),
            "total_investments": inv.get("totalInvestments"),
            "portfolio_count": inv.get("portfolioCoinsCount"),
            "last_update": datetime.utcnow().isoformat()
        }).execute()

def run_all():
    ingest_investors()
    print("✅ Finished Droptabs Investors ingestion.")

if __name__ == "__main__":
    run_all()
