import os
import requests
from datetime import datetime
from supabase import create_client

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DROPTABS_KEY = os.getenv("DROPTABS_KEY")

sb = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE_URL = "https://public-api.dropstab.com/api/v1"

HEADERS = {
    "accept": "application/json",
    "api_key": DROPTABS_KEY,          # ✅ Correct header
    "x-dropstab-api-key": DROPTABS_KEY  # ✅ Backup header (some endpoints need it)
}

def fetch_api(endpoint, params=None):
    url = f"{BASE_URL}/{endpoint}"
    resp = requests.get(url, headers=HEADERS, params=params or {})
    resp.raise_for_status()
    data = resp.json()
    return data.get("data", {}).get("content", []) or data.get("data", [])

# ---- Ingest Supported Coins ----
def ingest_supported_coins():
    print("📥 Fetching supported coins...")
    rows = fetch_api("tokenUnlocks/supportedCoins", {"pageSize": 50, "page": 0})
    for c in rows:
        # fallback ID: coinSlug → ensures id never null
        coin_id = c.get("id") or c.get("coinSlug") or c.get("slug")

        sb.table("droptabs_supported_coins").upsert({
            "id": str(coin_id),  # ensure never NULL
            "slug": c.get("coinSlug") or c.get("slug"),
            "symbol": c.get("coinSymbol") or c.get("symbol"),
            "name": c.get("name"),
            "last_update": datetime.utcnow().isoformat()
        }).execute()
    print(f"✅ Inserted {len(rows)} supported coins")

# ---- Ingest Unlocks ----
def ingest_unlocks():
    print("📥 Fetching unlocks...")
    rows = fetch_api("tokenUnlocks", {"pageSize": 50, "page": 0})
    for u in rows:
        sb.table("droptabs_unlocks").upsert({
            "coin_id": u.get("coin", {}).get("id"),
            "coin_slug": u.get("coin", {}).get("slug"),
            "coin_symbol": u.get("coin", {}).get("symbol"),
            "price_usd": u.get("coin", {}).get("priceUsd"),
            "market_cap": u.get("coin", {}).get("marketCap"),
            "fdv": u.get("coin", {}).get("fdv"),
            "circulation_supply_percent": u.get("coin", {}).get("circulatingSupplyPercent"),
            "unlocked_percent": u.get("unlockedPercent"),
            "locked_percent": u.get("lockedPercent"),
            "tge_date": u.get("tgeDate"),
            "last_update": datetime.utcnow().isoformat()
        }).execute()
    print(f"✅ Inserted {len(rows)} unlocks")

def run_all():
    print("🚀 Starting Droptabs ingestion...")
    ingest_supported_coins()
    ingest_unlocks()
    print("🎉 Finished Droptabs ingestion.")

if __name__ == "__main__":
    run_all()
