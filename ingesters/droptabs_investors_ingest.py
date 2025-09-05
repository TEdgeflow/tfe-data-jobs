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

def ingest_investors():
    print("🚀 Fetching investors...")
    rows = fetch_api("investors")

    for inv in rows:
        sb.table("droptabs_investors").upsert({
            "id": inv.get("id"),
            "investor_slug": inv.get("investorSlug"),
            "name": inv.get("name"),
            "rank": inv.get("rank"),
            "country": inv.get("country"),
            "description": inv.get("description"),
            "venture_type": inv.get("ventureType"),
            "tier": inv.get("tier"),
            "twitter_score": inv.get("twitterScore"),
            "last_round_date": inv.get("lastRoundDate"),
            "rounds_per_year": inv.get("roundsPerYear"),
            "public_sales_count": inv.get("publicSalesCount"),
            "retail_roi_percent": inv.get("retailRoiPercent"),
            "private_roi_percent": inv.get("privateRoiPercent"),
            "total_investments": inv.get("totalInvestments"),
            "lead_investments": inv.get("leadInvestments"),
            "portfolio_coins_count": inv.get("portfolioCoinsCount"),
            "last_update": datetime.utcnow().isoformat()
        }).execute()

    print(f"✅ Inserted {len(rows)} investors")

def run_all():
    ingest_investors()

if __name__ == "__main__":
    run_all()

