import os
import requests
from datetime import datetime, timezone
from supabase import create_client, Client

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DROPTABS_KEY = os.getenv("DROPTABS_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("❌ Missing Supabase credentials")

if not DROPTABS_KEY:
    raise RuntimeError("❌ Missing Droptabs API key")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ✅ Correct headers (this was the working setup)
HEADERS = {
    "accept": "application/json",
    "api_key": DROPTABS_KEY,
    "x-dropstab-api-key": DROPTABS_KEY
}
BASE_URL = "https://public-api.dropstab.com/api/v1"

# ========= HELPERS =========
def iso_now():
    return datetime.now(timezone.utc).isoformat()

def fetch_api(endpoint, params=None):
    url = f"{BASE_URL}/{endpoint}"
    print(f"📥 Fetching {url}")
    try:
        resp = requests.get(url, headers=HEADERS, params=params or {})
        resp.raise_for_status()
        data = resp.json()
        # return just the list content
        return data.get("data", {}).get("content", [])
    except Exception as e:
        print(f"❌ API error for {url}: {e}")
        return []

# ========= INGESTION =========
def ingest_supported_coins():
    rows = fetch_api("tokenUnlocks/supportedCoins", {"pageSize": 50, "page": 0})
    if not rows:
        print("⚠️ No supported coins returned")
        return
    formatted = []
    for c in rows:
        formatted.append({
            "id": c.get("id") or c.get("coinSlug"),  # ✅ fallback to slug
            "slug": c.get("coinSlug"),
            "symbol": c.get("coinSymbol"),
            "name": c.get("name"),
            "last_update": iso_now()
        })
    sb.table("droptabs_supported_coins").upsert(formatted).execute()
    print(f"✅ Inserted {len(formatted)} supported coins")

def ingest_unlocks():
    rows = fetch_api("tokenUnlocks", {"pageSize": 50, "page": 0})
    if not rows:
        print("⚠️ No unlocks returned")
        return
    formatted = []
    for u in rows:
        coin = u.get("coin", {}) if isinstance(u.get("coin"), dict) else {}
        formatted.append({
            "coin_id": coin.get("id") or coin.get("slug"),
            "coin_slug": coin.get("slug"),
            "coin_symbol": coin.get("symbol"),
            "price_usd": coin.get("priceUsd"),
            "market_cap": coin.get("marketCap"),
            "fdv": coin.get("fdv"),
            "circulation_supply_percent": coin.get("circulatingSupplyPercent"),
            "unlocked_percent": u.get("unlockedPercent"),
            "locked_percent": u.get("lockedPercent"),
            "tge_date": u.get("tgeDate"),
            "last_update": iso_now()
        })
    sb.table("droptabs_unlocks").upsert(formatted).execute()
    print(f"✅ Inserted {len(formatted)} unlocks")

# ========= MAIN =========
def run_all():
    print("🚀 Starting Droptabs ingestion...")
    ingest_supported_coins()
    ingest_unlocks()
    print("🎉 Finished Droptabs ingestion")

if __name__ == "__main__":
    run_all()

