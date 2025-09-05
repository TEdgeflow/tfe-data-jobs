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
    "x-dropstab-api-key": DROPTABS_KEY
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
    print(f"⬆️ Upserting {len(rows)} rows into {table}")
    try:
        sb.table(table).upsert(rows).execute()
    except Exception as e:
        print(f"❌ Supabase insert failed for {table}: {e}")

# ========= INGESTORS =========
def ingest_supported_coins():
    data = fetch_json("/tokenUnlocks/supportedCoins")
    if not data:
        return

    print("🔍 Raw supportedCoins response:", data)  # DEBUG

    rows = []
    for c in data.get("data", []):
        if isinstance(c, dict):
            rows.append({
                "slug": c.get("slug"),
                "symbol": c.get("symbol"),
                "name": c.get("name"),
                "last_update": iso_now()
            })
        elif isinstance(c, str):
            rows.append({
                "slug": c,
                "symbol": None,
                "name": None,
                "last_update": iso_now()
            })
        else:
            print(f"⚠️ Skipping unexpected supportedCoin: {c}")

    upsert("droptabs_supported_coins", rows)

def ingest_unlocks():
    data = fetch_json("/tokenUnlocks", {"pageSize": 100})
    if not data:
        return

    print("🔍 Raw unlocks response:", data)  # DEBUG

    rows = []
    for u in data.get("data", []):
        coin = u.get("coin")
        if isinstance(coin, dict):
            slug = coin.get("slug")
            symbol = coin.get("symbol")
        elif isinstance(coin, str):
            slug = coin
            symbol = None
        else:
            slug, symbol = None, None

        rows.append({
            "token": slug,
            "symbol": symbol,
            "unlock_date": u.get("date"),
            "amount": u.get("amount"),
            "category": u.get("category"),
            "last_update": iso_now()
        })

    upsert("droptabs_unlocks", rows)

def ingest_investors():
    data = fetch_json("/investors", {"pageSize": 100})
    if not data:
        return

    print("🔍 Raw investors response:", data)  # DEBUG

    rows = []
    for inv in data.get("data", []):
        if isinstance(inv, dict):
            rows.append({
                "slug": inv.get("slug"),
                "name": inv.get("name"),
                "portfolio_size": inv.get("portfolioSize"),
                "rounds_per_year": inv.get("roundsPerYear"),
                "last_update": iso_now()
            })
        else:
            print(f"⚠️ Skipping invalid investor: {inv}")

    upsert("droptabs_investors", rows)

def ingest_funding():
    data = fetch_json("/fundingRounds", {"pageSize": 100})
    if not data:
        return

    print("🔍 Raw funding response:", data)  # DEBUG

    rows = []
    for f in data.get("data", []):
        if isinstance(f, dict):
            rows.append({
                "id": f.get("id"),
                "project": f.get("project", {}).get("slug") if isinstance(f.get("project"), dict) else None,
                "amount": f.get("fundsRaised"),
                "date": f.get("date"),
                "round_type": f.get("roundType"),
                "last_update": iso_now()
            })
        else:
            print(f"⚠️ Skipping invalid funding record: {f}")

    upsert("droptabs_funding", rows)

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
