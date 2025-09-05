import os
import requests
from supabase import create_client, Client
from datetime import datetime

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DROPTABS_KEY = os.getenv("DROPTABS_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE_URL = "https://public-api.dropstab.com/api/v1"
HEADERS = {"api_key": DROPTABS_KEY}

def fetch_api(endpoint):
    url = f"{BASE_URL}/{endpoint}"
    print(f"🔗 Fetching {url}")
    resp = requests.get(url, headers=HEADERS, params={"pageSize": 50, "page": 0})
    resp.raise_for_status()
    data = resp.json()
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
            "last_update": datetime.utcnow().isoformat()
        }).execute()
    print(f"✅ Inserted {len(rows)} unlocks")

def run_all():
    ingest_supported_coins()
    ingest_unlocks()

if __name__ == "__main__":
    run_all()




