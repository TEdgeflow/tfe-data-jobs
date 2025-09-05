import os
import requests
from supabase import create_client, Client
from datetime import datetime

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DROPTABS_KEY = os.getenv("DROPTABS_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE_URL = "https://public-api.dropstab.com/api/v1"
HEADERS = {"api-key": DROPTABS_KEY}

def fetch_api(endpoint, params=None):
    url = f"{BASE_URL}/{endpoint}"
    resp = requests.get(url, headers=HEADERS, params=params or {"pageSize": 50, "page": 0})
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
            "image": inv.get("image"),
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
            "links": inv.get("links"),
            "last_update": datetime.utcnow().isoformat()
        }).execute()
    print(f"✅ Inserted {len(rows)} investors")

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
            "pre_valuation_inaccurate": fr.get("preValuationInaccurate"),
            "stage": fr.get("stage"),
            "twitter_performance": fr.get("twitterPerformance"),
            "category": fr.get("category"),
            "date": fr.get("date"),
            "investors": fr.get("investors"),  # stored as JSON
            "last_update": datetime.utcnow().isoformat()
        }).execute()
    print(f"✅ Inserted {len(rows)} funding rounds")

def run_all():
    ingest_investors()
    ingest_funding()

if __name__ == "__main__":
    run_all()
