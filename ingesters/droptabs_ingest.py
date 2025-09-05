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

# ---- Fetch API Helper ----
def fetch_api(endpoint, params=None):
    url = f"{BASE_URL}/{endpoint}"
    resp = requests.get(url, headers=HEADERS, params=params or {"pageSize": 5, "page": 0})
    resp.raise_for_status()
    data = resp.json()
    return data.get("data", {}).get("content", data.get("data", []))

# ---- Ingest Supported Coins ----
def ingest_supported_coins():
    print("📥 Fetching supported coins...")
    rows = fetch_api("tokenUnlocks/supportedCoins")
    print("🔍 Sample row from API:", rows[0] if rows else "EMPTY")

    for c in rows:
        try:
            sb.table("droptabs_supported_coins").upsert({
                "id": c.get("id"),
                "slug": c.get("coinSlug") or c.get("slug"),
                "symbol": c.get("coinSymbol") or c.get("symbol"),
                "last_update": datetime.utcnow().isoformat()
            }).execute()
        except Exception as e:
            print("⚠️ Insert failed:", e, "Row:", c)

    print(f"✅ Processed {len(rows)} supported coins")

# ---- Ingest Unlocks ----
def ingest_unlocks():
    print("📥 Fetching unlocks...")
    rows = fetch_api("tokenUnlocks")
    print("🔍 Sample unlock row:", rows[0] if rows else "EMPTY")

    for u in rows:
        try:
            sb.table("droptabs_unlocks").upsert({
                "id": u.get("id"),
                "coin_id": u.get("coinId"),
                "coin_slug": u.get("coinSlug"),
                "coin_symbol": u.get("coinSymbol"),
                "price_usd": u.get("priceUsd"),
                "market_cap": u.get("marketCap"),
                "fdv": u.get("fdv"),
                "circulation_supply_percent": u.get("circulatingSupplyPercent"),
                "unlocked_percent": u.get("unlockedPercent"),
                "locked_percent": u.get("lockedPercent"),
                "tge_date": u.get("tgeDate"),
                "last_update": datetime.utcnow().isoformat()
            }).execute()
        except Exception as e:
            print("⚠️ Insert failed:", e, "Row:", u)

    print(f"✅ Processed {len(rows)} unlocks")

# ---- Run All ----
def run_all():
    ingest_supported_coins()
    ingest_unlocks()

if __name__ == "__main__":
    run_all()

