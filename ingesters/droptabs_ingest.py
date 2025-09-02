import os
import time
import requests
from datetime import datetime, timezone
from supabase import create_client, Client

# ===== ENV VARS =====
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DROPTABS_KEY = os.getenv("DROPTABS_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing Supabase credentials")
if not DROPTABS_KEY:
    raise RuntimeError("Missing Droptabs API key")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

HEADERS = {"x-droptab-api-key": DROPTABS_KEY}
BASE_URL = "https://public-api.droptab.com/api/v1"

# ===== HELPERS =====
def iso_now():
    return datetime.now(timezone.utc).isoformat()

def fetch_json(endpoint, params=None):
    url = f"{BASE_URL}{endpoint}"
    r = requests.get(url, headers=HEADERS, params=params or {})
    try:
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[error] API failed: {url}, status={r.status_code}, body={r.text}")
        return None

def upsert(table, rows):
    if not rows:
        print(f"[warn] No data to upsert into {table}")
        return
    try:
        res = sb.table(table).upsert(rows).execute()
        print(f"[ok] Upserted {len(rows)} rows into {table}")
    except Exception as e:
        print(f"[error] Supabase insert failed for {table}: {e}")

# ===== INGEST FUNCTIONS =====
def ingest_unlocks():
    data = fetch_json("/tokenUnlocks", {"page": 0, "pageSize": 100})
    if not data:
        return
    rows = []
    for u in data.get("data", []):
        rows.append({
            "coin": u.get("coin", {}).get("symbol"),
            "unlock_date": u.get("date"),
            "amount": u.get("amount"),
            "category": u.get("category"),
            "timestamp": iso_now()
        })
    upsert("droptabs_unlocks", rows)

def ingest_supported_coins():
    data = fetch_json("/tokenUnlocks/supportedCoins")
    if not data:
        return
    rows = [{"slug": c.get("slug"), "name": c.get("name"), "symbol": c.get("symbol"), "timestamp": iso_now()} for c in data.get("data", [])]
    upsert("droptabs_supported_coins", rows)

def ingest_investors():
    data = fetch_json("/investors", {"page": 0, "pageSize": 100})
    if not data:
        return
    rows = [{"slug": i.get("slug"), "name": i.get("name"), "portfolio_size": i.get("portfolioSize"), "timestamp": iso_now()} for i in data.get("data", [])]
    upsert("droptabs_investors", rows)

def ingest_funding_rounds():
    data = fetch_json("/fundingRounds", {"page": 0, "pageSize": 100})
    if not data:
        return
    rows = []
    for f in data.get("data", []):
        rows.append({
            "project": f.get("project", {}).get("name"),
            "date": f.get("date"),
            "amount": f.get("fundsRaised"),
            "round": f.get("roundType"),
            "timestamp": iso_now()
        })
    upsert("droptabs_funding_rounds", rows)

# ===== MAIN =====
if __name__ == "__main__":
    print("Starting Droptabs ingestion...")
    ingest_unlocks()
    ingest_supported_coins()
    ingest_investors()
    ingest_funding_rounds()
    print("✅ Done")

