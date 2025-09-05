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
    "x-dropstab-api-key": DROPTABS_KEY  # ✅ Correct header
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
def ingest_funding_rounds():
    data = fetch_json("/fundingRounds", {"pageSize": 100})
    if not data:
        return

    rows = []
    for f in data.get("data", []):
        if isinstance(f, dict):
            rows.append({
                "project": f.get("project", {}).get("slug") if isinstance(f.get("project"), dict) else None,
                "amount": f.get("fundsRaised"),
                "date": f.get("date"),
                "round_type": f.get("roundType"),
                "last_update": iso_now()
            })

    upsert("droptabs_funding", rows)

# ========= MAIN =========
if __name__ == "__main__":
    print("🚀 Starting Droptabs Funding ingestion...")
    ingest_funding_rounds()
    print("✅ Finished Droptabs Funding ingestion.")
