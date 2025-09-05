import os
import requests
from supabase import create_client, Client
from datetime import datetime

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DROPTABS_KEY = os.getenv("DROPTABS_KEY")

if not SUPABASE_URL or not SUPABASE_KEY or not DROPTABS_KEY:
    raise RuntimeError("Missing SUPABASE_URL, SUPABASE_KEY or DROPTABS_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE_URL = "https://public-api.dropstab.com/api/v1"

HEADERS = {
    "accept": "application/json",
    "x-dropstab-api-key": DROPTABS_KEY
}

# ========= HELPERS =========
def log(msg):
    print(f"🔹 {msg}")

def fetch(url, params=None):
    try:
        response = requests.get(url, headers=HEADERS, params=params)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        log(f"❌ Failed request {url}: {e}")
        return None

# ========= INGEST SUPPORTED COINS =========
def ingest_supported_coins():
    log("Fetching supportedCoins...")
    data = fetch(f"{BASE_URL}/tokenUnlocks/supportedCoins", {"pageSize": 100})
    if not data or "data" not in data:
        log("⚠️ No data for supportedCoins")
        return
    
    rows = []
    for item in data["data"].get("content", []):
        rows.append({
            "slug": item.get("slug"),
            "symbol": item.get("symbol"),
            "name": item.get("name"),
            "last_update": datetime.utcnow().isoformat()
        })

    if rows:
        sb.table("droptabs_supported_coins").upsert(rows).execute()
        log(f"✅ Inserted {len(rows)} rows into droptabs_supported_coins")
    else:
        log("⚠️ No rows to insert for supportedCoins")

# ========= INGEST UNLOCKS =========
def ingest_unlocks():
    log("Fetching unlocks...")
    data = fetch(f"{BASE_URL}/tokenUnlocks", {"pageSize": 50})
    if not data or "data" not in data:
        log("⚠️ No data for unlocks")
        return

    rows = []
    for u in data["data"].get("content", []):
        rows.append({
            "coin_slug": u.get("coinSlug"),
            "coin_symbol": u.get("coinSymbol"),
            "market_cap": u.get("marketCap"),
            "circulation_supply_percent": u.get("circulationSupplyPercent"),
            "total_tokens_unlocked_percent": u.get("totalTokensUnlockedPercent"),
            "total_tokens_locked_percent": u.get("totalTokensLockedPercent"),
            "tge_date": u.get("tgeDate"),
            "last_update": datetime.utcnow().isoformat()
        })

    if rows:
        sb.table("droptabs_unlocks").upsert(rows).execute()
        log(f"✅ Inserted {len(rows)} rows into droptabs_unlocks")
    else:
        log("⚠️ No rows to insert for unlocks")

# ========= INGEST INVESTORS =========
def ingest_investors():
    log("Fetching investors...")
    data = fetch(f"{BASE_URL}/investors", {"pageSize": 50})
    if not data or "data" not in data:
        log("⚠️ No data for investors")
        return

    rows = []
    for inv in data["data"].get("content", []):
        rows.append({
            "investor_slug": inv.get("slug"),
            "name": inv.get("name"),
            "portfolio_size": inv.get("portfolioSize"),
            "roi": inv.get("roi"),
            "rounds_per_year": inv.get("roundsPerYear"),
            "last_update": datetime.utcnow().isoformat()
        })

    if rows:
        sb.table("droptabs_investors").upsert(rows).execute()
        log(f"✅ Inserted {len(rows)} rows into droptabs_investors")
    else:
        log("⚠️ No rows to insert for investors")

# ========= INGEST FUNDING =========
def ingest_funding():
    log("Fetching funding rounds...")
    data = fetch(f"{BASE_URL}/fundingRounds", {"pageSize": 50})
    if not data or "data" not in data:
        log("⚠️ No data for funding rounds")
        return

    rows = []
    for f in data["data"].get("content", []):
        rows.append({
            "project": f.get("projectName"),
            "amount": f.get("amountRaised"),
            "date": f.get("date"),
            "round_type": f.get("roundType"),
            "last_update": datetime.utcnow().isoformat()
        })

    if rows:
        sb.table("droptabs_funding").upsert(rows).execute()
        log(f"✅ Inserted {len(rows)} rows into droptabs_funding")
    else:
        log("⚠️ No rows to insert for funding")

# ========= MAIN =========
def run_all():
    ingest_supported_coins()
    ingest_unlocks()
    ingest_investors()
    ingest_funding()

if __name__ == "__main__":
    run_all()



