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
    "Authorization": f"Bearer {DROPTABS_KEY}"  # ✅ FIXED
}
BASE_URL = "https://public-api.dropstab.com/api/v1"

# ========= HELPERS =========
def iso_now():
    return datetime.now(timezone.utc).isoformat()

def fetch_json(endpoint, params=None):
    url = f"{BASE_URL}{endpoint}"
    print(f"🔗 Fetching {url}")
    r = requests.get(url, headers=HEADERS, params=params or {})
    if r.status_code != 200:
        print(f"❌ API error {r.status_code} for {url}: {r.text}")
        return None
    try:
        return r.json()
    except Exception as e:
        print(f"❌ Failed to parse JSON from {url}: {e}")
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

# ========= INGESTION =========
def ingest_unlocks():
    data = fetch_json("/tokenUnlocks", {"pageSize": 100})
    if not data:
        return

    rows = []
    for u in data.get("data", []):
        coin = u.get("coin") if isinstance(u.get("coin"), dict) else {}
        rows.append({
            "token": coin.get("slug"),
            "symbol": coin.get("symbol"),
            "unlock_date": u.get("date"),
            "amount": u.get("amount"),
            "category": u.get("category"),
            "last_update": iso_now()
        })

    upsert("droptabs_unlocks", rows)

def ingest_supported_coins():
    data = fetch_json("/tokenUnlocks/supportedCoins")
    if not data:
        return

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

    upsert("droptabs_supported_coins", rows)

# ========= MAIN =========
def run_all():
    print("🚀 Starting Droptabs Unlocks ingestion...")
    ingest_supported_coins()
    ingest_unlocks()
    print("✅ Finished Droptabs Unlocks ingestion.")

if __name__ == "__main__":
    run_all()
