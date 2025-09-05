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

HEADERS = {
    "accept": "application/json",
    "x-droptab-api-key": DROPTABS_KEY  # ✅ working header (do not change)
}
BASE_URL = "https://public-api.dropstab.com/api/v1"

# ========= HELPERS =========
def iso_now():
    return datetime.now(timezone.utc).isoformat()

def fetch_json(endpoint, params=None):
    url = f"{BASE_URL}{endpoint}"
    print(f"🔗 Fetching {url}")
    try:
        r = requests.get(url, headers=HEADERS, params=params or {})
        if r.status_code != 200:
            print(f"❌ API error {r.status_code} for {url}: {r.text}")
            return None
        return r.json()
    except Exception as e:
        print(f"❌ Request failed for {url}: {e}")
        return None

def upsert(table, rows):
    if not rows:
        print(f"⚠️ No rows to upsert into {table}")
        return
    try:
        sb.table(table).upsert(rows).execute()
        print(f"⬆️ Inserted {len(rows)} rows into {table}")
    except Exception as e:
        print(f"❌ Supabase insert failed for {table}: {e}")

# ========= INGESTION =========
def ingest_supported_coins():
    data = fetch_json("/tokenUnlocks/supportedCoins", {"pageSize": 50, "page": 0})
    if not data or "data" not in data:
        return

    rows = []
    for c in data["data"]:
        rows.append({
            "coin_id": c.get("id"),
            "coin_slug": c.get("slug"),
            "coin_symbol": c.get("symbol"),
            "coin_name": c.get("name"),
            "last_update": iso_now()
        })
    upsert("droptabs_supported_coins", rows)

def ingest_unlocks():
    data = fetch_json("/tokenUnlocks", {"pageSize": 50, "page": 0})
    if not data or "data" not in data:
        return

    rows = []
    for u in data["data"].get("content", []):  # ✅ unlocks use content list
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
            "last_update": iso_now()
        })
    upsert("droptabs_unlocks", rows)

def ingest_investors():
    data = fetch_json("/investors", {"pageSize": 50, "page": 0})
    if not data or "data" not in data:
        return

    rows = []
    for inv in data["data"]:
        rows.append({
            "slug": inv.get("slug"),
            "name": inv.get("name"),
            "portfolio_size": inv.get("portfolioSize"),
            "rounds_per_year": inv.get("roundsPerYear"),
            "last_update": iso_now()
        })
    upsert("droptabs_investors", rows)

def ingest_funding():
    data = fetch_json("/fundingRounds", {"pageSize": 50, "page": 0})
    if not data or "data" not in data:
        return

    rows = []
    for f in data["data"]:
        rows.append({
            "id": f.get("id"),
            "project": f.get("project", {}).get("slug") if isinstance(f.get("project"), dict) else None,
            "amount": f.get("fundsRaised"),
            "date": f.get("date"),
            "round_type": f.get("roundType"),
            "last_update": iso_now()
        })
    upsert("droptabs_funding", rows)

# ========= MAIN =========
def run_all():
    print("🚀 Starting Droptabs ingestion...")

    print("📌 Supported Coins...")
    ingest_supported_coins()

    print("📌 Unlocks...")
    ingest_unlocks()

    print("📌 Investors...")
    ingest_investors()

    print("📌 Funding Rounds...")
    ingest_funding()

    print("✅ Finished Droptabs ingestion.")

if __name__ == "__main__":
    run_all()

