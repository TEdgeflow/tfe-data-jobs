import os
import requests
from supabase import create_client

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DROPTABS_KEY = os.getenv("DROPTABS_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE_URL = "https://public-api.dropstab.com/api/v1"

HEADERS = {
    "accept": "application/json",
    "x-dropstab-api-key": DROPTABS_KEY
}

# ========= FETCH FUNCTION =========
def fetch_unlocks(page_size=50):
    url = f"{BASE_URL}/tokenUnlocks?pageSize={page_size}"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    return response.json()

# ========= MAIN INGEST =========
def run_all():
    print("🚀 Fetching unlocks...")
    data = fetch_unlocks(page_size=50)
    rows = []

    for u in data.get("data", {}).get("content", []):
        rows.append({
            "coin_id": u.get("coinId"),
            "coin_slug": u.get("coinSlug"),
            "coin_symbol": u.get("coinSymbol"),
            "price_usd": u.get("priceUsd"),
            "market_cap": u.get("marketCap"),
            "fdv": u.get("fdv"),
            "circulation_supply_percent": u.get("circulationSupplyPercent"),
            "unlocked_percent": u.get("totalTokensUnlockedPercent"),
            "locked_percent": u.get("totalTokensLockedPercent"),
            "tge_date": u.get("tgeDate"),
            "unlock_date": u.get("unlockDate") or None,  # 👈 NEW: capture unlock date if available
            "last_update": u.get("updatedAt")
        })

    if rows:
        supabase.table("droptabs_unlocks").upsert(rows).execute()
        print(f"✅ Inserted {len(rows)} rows (with unlock_date if available)")
    else:
        print("⚠️ No rows to insert")

# ========= RUN SCRIPT =========
if __name__ == "__main__":
    run_all()


