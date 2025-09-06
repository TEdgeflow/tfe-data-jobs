import os
import requests
from datetime import datetime
from supabase import create_client

# ---- ENV VARS ----
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DROPTABS_KEY = os.getenv("DROPTABS_KEY")

sb = create_client(SUPABASE_URL, SUPABASE_KEY)

DROPTABS_BASE = "https://public-api.dropstab.com/api/v1"

# ---- Helper to fetch API ----
def fetch_api(endpoint, page=0, page_size=50):
    url = f"{DROPTABS_BASE}/{endpoint}"
    headers = {
        "api_key": DROPTABS_KEY,             # ✅ required
        "x-dropstab-api-key": DROPTABS_KEY   # ✅ also required
    }
    params = {"pageSize": page_size, "page": page}
    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    data = resp.json()
    # ✅ Fix: real rows are under data.content
    return data.get("data", {}).get("content", [])

# ---- Ingest Supported Coins ----
def ingest_supported_coins():
    print("📥 Fetching supported coins...")
    rows = fetch_api("tokenUnlocks/supportedCoins")
    for c in rows:
        sb.table("droptabs_supported_coins").upsert({
            "id": c.get("id"),
            "slug": c.get("coinSlug") or c.get("slug"),
            "symbol": c.get("coinSymbol") or c.get("symbol"),
            "name": c.get("name"),
            "last_update": datetime.utcnow().isoformat()
        }).execute()
    print(f"✅ Inserted {len(rows)} supported coins")

# ---- Ingest Unlocks ----
def ingest_unlocks():
    print("📥 Fetching unlocks...")
    rows = fetch_api("tokenUnlocks")
    for u in rows:
        sb.table("droptabs_unlocks").upsert({
            "id": u.get("id"),
            "coin_id": u.get("coin", {}).get("id"),
            "coin_slug": u.get("coin", {}).get("slug"),
            "coin_symbol": u.get("coin", {}).get("symbol"),
            "price_usd": u.get("priceUsd"),
            "market_cap": u.get("marketCap"),
            "fdv": u.get("fdv"),
            "circulation_supply_percent": u.get("circulationSupplyPercent"),
            "unlocked_percent": u.get("unlockedPercent"),
            "locked_percent": u.get("lockedPercent"),
            "tge_date": u.get("tgeDate"),
            "last_update": datetime.utcnow().isoformat()
        }).execute()
    print(f"✅ Inserted {len(rows)} unlocks")

# ---- Run All ----
def run_all():
    print("🚀 Starting Droptabs ingestion...")
    ingest_supported_coins()
    ingest_unlocks()
    print("✅ Finished Droptabs ingestion.")

if __name__ == "__main__":
    run_all()
