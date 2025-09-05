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
    "x-api-key": DROPTABS_KEY  # ✅ correct header
}
BASE_URL = "https://public-api.dropstab.com/api/v1"

# ========= HELPERS =========
def iso_now():
    return datetime.now(timezone.utc).isoformat()

def fetch_all(endpoint, page_size=50):
    """Fetch all pages from Droptabs API"""
    all_rows = []
    page = 0
    while True:
        url = f"{BASE_URL}{endpoint}?pageSize={page_size}&page={page}"
        print(f"🔗 Fetching {url}")
        r = requests.get(url, headers=HEADERS)
        if r.status_code != 200:
            print(f"❌ API error {r.status_code}: {r.text}")
            break

        data = r.json().get("data", {})
        rows = data.get("content", [])
        if not rows:
            break

        all_rows.extend(rows)

        if data.get("currentPage", 0) >= data.get("totalPages", 1) - 1:
            break
        page += 1
    return all_rows

def upsert(table, rows):
    if not rows:
        print(f"⚠️ No rows to upsert into {table}")
        return
    print(f"⬆️ Upserting {len(rows)} rows into {table}")
    sb.table(table).upsert(rows).execute()

# ========= INGESTIONS =========
def ingest_supported_coins():
    rows = fetch_all("/tokenUnlocks/supportedCoins")
    parsed = []
    for c in rows:
        if isinstance(c, dict):
            parsed.append({
                "coin_id": c.get("id"),
                "coin_slug": c.get("slug"),
                "coin_symbol": c.get("symbol"),
                "coin_name": c.get("name"),
                "last_update": iso_now()
            })
    upsert("droptabs_supported_coins", parsed)

def ingest_unlocks():
    rows = fetch_all("/tokenUnlocks")
    parsed = []
    for u in rows:
        parsed.append({
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
    upsert("droptabs_unlocks", parsed)

def ingest_investors():
    rows = fetch_all("/investors")
    parsed = []
    for inv in rows:
        parsed.append({
            "investor_id": inv.get("id"),
            "name": inv.get("name"),
            "slug": inv.get("slug"),
            "portfolio_size": inv.get("portfolioSize"),
            "rounds_per_year": inv.get("roundsPerYear"),
            "last_update": iso_now()
        })
    upsert("droptabs_investors", parsed)

def ingest_funding():
    rows = fetch_all("/fundingRounds")
    parsed = []
    for f in rows:
        project = f.get("project") if isinstance(f.get("project"), dict) else {}
        parsed.append({
            "funding_id": f.get("id"),
            "project": project.get("slug"),
            "amount": f.get("fundsRaised"),
            "date": f.get("date"),
            "round_type": f.get("roundType"),
            "last_update": iso_now()
        })
    upsert("droptabs_funding", parsed)

# ========= MAIN =========
def run_all():
    print("🚀 Starting Droptabs ingestion...")
    ingest_supported_coins()
    ingest_unlocks()
    ingest_investors()
    ingest_funding()
    print("✅ Finished Droptabs ingestion.")

if __name__ == "__main__":
    run_all()




