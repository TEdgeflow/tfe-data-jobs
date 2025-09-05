import os
import requests
from supabase import create_client, Client
from datetime import datetime

# === ENV VARS ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DROPTABS_KEY = os.getenv("DROPTABS_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE_URL = "https://public-api.dropstab.com/api/v1"
HEADERS = {"api_key": DROPTABS_KEY}  # ✅ Investors require header auth

def fetch_investors(page=0, page_size=50):
    url = f"{BASE_URL}/investors"
    params = {"pageSize": page_size, "page": page}
    resp = requests.get(url, headers=HEADERS, params=params)
    resp.raise_for_status()
    data = resp.json()
    return data.get("data", {}).get("content", [])

def ingest_investors():
    print("📥 Fetching investors...")
    page = 0
    total_inserted = 0

    while True:
        rows = fetch_investors(page=page)
        if not rows:
            break

        inserts = []
        for inv in rows:
            inserts.append({
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
                "last_update": datetime.utcnow().isoformat()
            })

        if inserts:
            sb.table("droptabs_investors").upsert(inserts).execute()
            total_inserted += len(inserts)
            print(f"✅ Inserted {len(inserts)} rows (page {page})")

        page += 1

    print(f"🎉 Finished. Total investors inserted: {total_inserted}")

if __name__ == "__main__":
    ingest_investors()
