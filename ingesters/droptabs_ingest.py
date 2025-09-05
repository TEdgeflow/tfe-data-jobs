import os
import requests
from supabase import create_client, Client
from datetime import datetime

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DROPTABS_KEY = os.getenv("DROPTABS_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE_URL = "https://public-api.dropstab.com/api/v1"
HEADERS = {"api_key": DROPTABS_KEY}

def fetch_api(endpoint, params=None):
    url = f"{BASE_URL}/{endpoint}"
    resp = requests.get(url, headers=HEADERS, params=params or {"pageSize": 50, "page": 0})
    resp.raise_for_status()
    data = resp.json()
    return data.get("data", {}).get("content", [])

# ===================== INGEST FUNCTIONS =====================

def ingest_supported_coins():
    print("📥 Fetching supported coins...")
    rows = fetch_api("tokenUnlocks/supportedCoins")
    for r in rows:
        sb.table("droptabs_supported_coins").upsert({
            "id": r.get("id"),
            "slug": r.get("coin", {}).get("slug"),
            "symbol": r.get("coin", {}).get("symbol"),
            "last_update": datetime.utcnow().isoformat()
        }).execute()

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
            "circulation_supply_percent": u.get("circulatingSupplyPercent"),
            "unlocked_percent": u.get("unlockedPercent"),
            "locked_percent": u.get("lockedPercent"),
            "tge_date": u.get("tgeDate"),
            "last_update": datetime.utcnow().isoformat()
        }).execute()

# ===================== MAIN =====================

def run_all():
    ingest_supported_coins()
    ingest_unlocks()

if __name__ == "__main__":
    run_all()

