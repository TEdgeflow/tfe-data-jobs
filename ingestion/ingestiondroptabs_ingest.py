import os, requests
from datetime import datetime, timezone
from supabase import create_client, Client

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DROPTABS_KEY = os.getenv("DROPTABS_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

HEADERS = {
    "accept": "application/json",
    "x-dropstab-api-key": DROPTABS_KEY
}
BASE_URL = "https://public-api.dropstab.com/api/v1"

def iso_now():
    return datetime.now(timezone.utc).isoformat()

def fetch_json(endpoint, params=None):
    url = f"{BASE_URL}{endpoint}"
    print(f"🔗 Fetching {url}")
    r = requests.get(url, headers=HEADERS, params=params or {})
    if r.status_code != 200:
        print(f"❌ API error {r.status_code}: {r.text}")
        return None
    return r.json()

def upsert(table, rows):
    if not rows: return
    sb.table(table).upsert(rows).execute()

def ingest_unlocks():
    data = fetch_json("/tokenUnlocks", {"pageSize": 100})
    if not data: return
    rows = []
    for u in data.get("data", []):
        rows.append({
            "token": u.get("coin", {}).get("slug"),
            "symbol": u.get("coin", {}).get("symbol"),
            "unlock_date": u.get("date"),
            "amount": u.get("amount"),
            "category": u.get("category"),
            "last_update": iso_now()
        })
    upsert("droptabs_unlocks", rows)

def ingest_supported_coins():
    data = fetch_json("/tokenUnlocks/supportedCoins")
    if not data: return
    rows = []
    for c in data.get("data", []):
        rows.append({
            "slug": c.get("slug"),
            "symbol": c.get("symbol"),
            "name": c.get("name"),
            "last_update": iso_now()
        })
    upsert("droptabs_supported_coins", rows)

if __name__ == "__main__":
    print("🚀 Droptabs Unlocks ingestion...")
    ingest_supported_coins()
    ingest_unlocks()
    print("✅ Done unlocks")







