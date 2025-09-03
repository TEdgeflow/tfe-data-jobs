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

def ingest_funding_rounds():
    data = fetch_json("/fundingRounds", {"pageSize": 100})
    if not data: return
    rows = []
    for f in data.get("data", []):
        rows.append({
            "id": f.get("id"),
            "project": f.get("project", {}).get("slug"),
            "amount": f.get("fundsRaised"),
            "date": f.get("date"),
            "round_type": f.get("roundType"),
            "last_update": iso_now()
        })
    upsert("droptabs_funding_rounds", rows)

if __name__ == "__main__":
    print("🚀 Droptabs Funding Rounds ingestion...")
    ingest_funding_rounds()
    print("✅ Done funding rounds")
