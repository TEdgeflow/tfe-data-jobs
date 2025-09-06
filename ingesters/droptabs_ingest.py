import os
import requests
from datetime import datetime
from supabase import create_client, Client

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DROPTABS_KEY = os.getenv("DROPTABS_KEY")

if not SUPABASE_URL or not SUPABASE_KEY or not DROPTABS_KEY:
    raise RuntimeError("Missing SUPABASE_URL, SUPABASE_KEY, or DROPTABS_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE_URL = "https://public-api.dropstab.com/api/v1"

# ========= HELPERS =========
def fetch_api(endpoint: str, page_size: int = 50):
    """Fetch a page from Dropstab API."""
    url = f"{BASE_URL}/{endpoint}?pageSize={page_size}&page=0"
    headers = {"api_key": DROPTABS_KEY}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data.get("data", []) or data.get("content", [])


# ========= INGEST SUPPORTED COINS =========
def ingest_supported_coins():
    print("📥 Fetching supported coins...")
    rows = fetch_api("tokenUnlocks/supportedCoins")
    for c in rows:
        sb.table("droptabs_supported_coins").upsert({
            "id": c.get("id"),
            "slug": c.get("slug"),        # ✅ matches API
            "symbol": c.get("symbol"),    # ✅ matches API
            "name": c.get("name"),        # ✅ matches API
            "last_update": datetime.utcnow().isoformat()
        }).execute()
    print(f"✅ Inserted {len(rows)} supported coins")


# ========= INGEST UNLOCKS =========
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


# ========= RUN ALL =========
def run_all():
    ingest_supported_coins()
    ingest_unlocks()


if __name__ == "__main__":
    run_all()



