import os, requests
from datetime import datetime, timezone
from supabase import create_client, Client

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

def ingest_investors():
    data = fetch_json("/investors", {"pageSize": 100})
    if not data: return
    rows = []
    for inv in data.get("data", []):
        rows.append({
            "slug": inv.get("slug"),
            "name": inv.get("name"),
            "portfolio_size": inv.get("portfolioSize"),
            "rounds_per_year": inv.get("roundsPerYear"),
            "last_update": iso_now()
        })
    upsert("droptabs_investors", rows)

if __name__ == "__main__":
    print("🚀 Droptabs Investors ingestion...")
    ingest_investors()
    print("✅ Done investors")
