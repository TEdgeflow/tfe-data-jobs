import os
import time
import requests
from datetime import datetime, timezone
from supabase import create_client, Client

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DROPTABS_KEY = os.getenv("DROPTABS_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing Supabase credentials")
if not DROPTABS_KEY:
    raise RuntimeError("Missing DROPTABS_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

HEADERS = {"Authorization": f"Bearer {DROPTABS_KEY}"}
BASE_URL = "https://api.droptabs.io/v1"

def iso_now():
    return datetime.now(timezone.utc).isoformat()

def fetch_json(url, params=None):
    r = requests.get(url, headers=HEADERS, params=params)
    if r.status_code == 403:
        raise RuntimeError("403 Forbidden – check Droptabs plan or key permissions.")
    r.raise_for_status()
    return r.json()

# ========= INGEST UNLOCKS =========
def ingest_unlocks():
    url = f"{BASE_URL}/unlocks"
    data = fetch_json(url)

    rows = []
    for d in data.get("data", []):
        rows.append({
            "token": d.get("symbol"),
            "unlock_date": d.get("date"),
            "unlock_amount": d.get("amount"),
            "unlock_percent": d.get("percent"),
            "market_cap": d.get("marketCap"),
            "circulating_supply": d.get("circulatingSupply"),
            "timestamp": iso_now()
        })

    if rows:
        sb.table("droptabs_unlocks").upsert(rows).execute()
        print(f"[Droptabs Unlocks] Upserted {len(rows)} rows")

# ========= INGEST ACCUMULATION =========
def ingest_accumulation():
    url = f"{BASE_URL}/accumulation"
    data = fetch_json(url)

    rows = []
    for d in data.get("data", []):
        rows.append({
            "token": d.get("symbol"),
            "top_holders": d.get("topHolders"),
            "accumulation_change": d.get("accumulationChange"),
            "whale_ratio": d.get("whaleRatio"),
            "timestamp": iso_now()
        })

    if rows:
        sb.table("droptabs_accumulation").upsert(rows).execute()
        print(f"[Droptabs Accumulation] Upserted {len(rows)} rows")

# ========= MAIN LOOP =========
if __name__ == "__main__":
    while True:
        try:
            ingest_unlocks()
            ingest_accumulation()
        except Exception as e:
            print("[error]", e)
        time.sleep(3600)  # run every hour
