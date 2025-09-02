import os
import time
import requests
from datetime import datetime, timezone
from supabase import create_client, Client

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DROPTABS_KEY = os.getenv("DROPTABS_KEY")

# Allow BASE_URL override from env (default is droptabs.io/api/v1)
DROPTABS_BASE_URL = os.getenv("DROPTABS_BASE_URL", "https://droptabs.io/api/v1")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing Supabase credentials")
if not DROPTABS_KEY:
    raise RuntimeError("Missing DROPTABS_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

HEADERS = {"Authorization": f"Bearer {DROPTABS_KEY}"}

def iso_now():
    return datetime.now(timezone.utc).isoformat()

def fetch_json(endpoint, params=None):
    url = f"{DROPTABS_BASE_URL}{endpoint}"
    r = requests.get(url, headers=HEADERS, params=params)
    if r.status_code == 403:
        raise RuntimeError("403 Forbidden – check Droptabs plan or key permissions.")
    r.raise_for_status()
    return r.json()

# ========= INGEST UNLOCKS =========
def ingest_unlocks():
    data = fetch_json("/tokenUnlocks")
    rows = []
    for d in data.get("data", []):
        rows.append({
            "token": d.get("symbol"),
            "unlock_date": d.get("unlockDate"),
            "unlock_amount": d.get("amount"),
            "unlock_percent": d.get("percent"),
            "market_cap": d.get("marketCap"),
            "circulating_supply": d.get("circulatingSupply"),
            "timestamp": iso_now()
        })
    if rows:
        sb.table("droptabs_unlocks").upsert(rows).execute()
        print(f"[Droptabs Unlocks] Upserted {len(rows)} rows")

# ========= INGEST SUPPORTED COINS =========
def ingest_supported_coins():
    data = fetch_json("/tokenUnlocks/supportedCoins")
    rows = []
    for d in data.get("data", []):
        rows.append({
            "token": d.get("symbol"),
            "slug": d.get("slug"),
            "name": d.get("name"),
            "timestamp": iso_now()
        })
    if rows:
        sb.table("droptabs_supported_coins").upsert(rows).execute()
        print(f"[Droptabs Supported Coins] Upserted {len(rows)} rows")

# ========= INGEST INVESTORS =========
def ingest_investors():
    data = fetch_json("/investors")
    rows = []
    for d in data.get("data", []):
        rows.append({
            "slug": d.get("slug"),
            "name": d.get("name"),
            "category": d.get("category"),
            "website": d.get("website"),
            "timestamp": iso_now()
        })
    if rows:
        sb.table("droptabs_investors").upsert(rows).execute()
        print(f"[Droptabs Investors] Upserted {len(rows)} rows")

# ========= INGEST FUNDING ROUNDS =========
def ingest_funding_rounds():
    data = fetch_json("/fundingRounds")
    rows = []
    for d in data.get("data", []):
        rows.append({
            "id": d.get("id"),
            "project": d.get("project"),
            "amount": d.get("amount"),
            "stage": d.get("stage"),
            "date": d.get("date"),
            "timestamp": iso_now()
        })
    if rows:
        sb.table("droptabs_funding_rounds").upsert(rows).execute()
        print(f"[Droptabs FundingRounds] Upserted {len(rows)} rows")

# ========= MAIN LOOP =========
if __name__ == "__main__":
    while True:
        try:
            ingest_unlocks()
            ingest_supported_coins()
            ingest_investors()
            ingest_funding_rounds()
        except Exception as e:
            print("[error]", e)
        time.sleep(3600)  # run every hour
