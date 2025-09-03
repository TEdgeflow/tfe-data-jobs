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
    "x-droptab-api-key": DROPTABS_KEY
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
def ingest_investors():
    data = fetch_json("/investors", {"pageSize": 100})
    if not data:
        return

    print("🔍 Raw investors response:", data)  # DEBUG

    rows = []
    for inv in data.get("data", []):
        if isinstance(inv, dict):   # ✅ Only handle dicts
            rows.append({
                "slug": inv.get("slug"),
                "name": inv.get("name"),
                "portfolio_size": inv.get("portfolioSize"),
                "rounds_per_year": inv.get("roundsPerYear"),
                "last_update": iso_now()
            })
        else:
            print(f"⚠️ Skipping non-dict item in investors: {inv}")

    upsert("droptabs_investors", rows)

# ========= MAIN =========
if __name__ == "__main__":
    print("🚀 Starting Droptabs Investors ingestion...")
    ingest_investors()
    print("✅ Finished Droptabs Investors ingestion.")

