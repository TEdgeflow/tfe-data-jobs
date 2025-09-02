import os
import time
import requests
from datetime import datetime, timezone
from supabase import create_client, Client

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
COINAPI_KEY = os.getenv("COINAPI_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing Supabase credentials")
if not COINAPI_KEY:
    raise RuntimeError("Missing COINAPI_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

HEADERS = {"X-CoinAPI-Key": COINAPI_KEY}
BASE_URL = "https://rest.coinapi.io/v1"

def iso_now():
    return datetime.now(timezone.utc).isoformat()

def fetch_json(url, params=None):
    r = requests.get(url, headers=HEADERS, params=params)
    if r.status_code == 403:
        raise RuntimeError("403 Forbidden – your CoinAPI plan may not include this endpoint.")
    r.raise_for_status()
    return r.json()

def ingest_open_interest():
    url = f"{BASE_URL}/derivatives/openinterest"
    data = fetch_json(url)

    rows = []
    for d in data:
        rows.append({
            "symbol": d.get("symbol_id"),
            "oi": d.get("open_interest_usd"),
            "timestamp": d.get("time") or iso_now()
        })

    if rows:
        sb.table("coinapi_oi").upsert(rows).execute()
        print(f"[CoinAPI OI] Upserted {len(rows)} rows")

def ingest_funding():
    url = f"{BASE_URL}/derivatives/funding_rates"
    data = fetch_json(url)

    rows = []
    for d in data:
        rows.append({
            "symbol": d.get("symbol_id"),
            "funding_rate": d.get("funding_rate"),
            "timestamp": d.get("time") or iso_now()
        })

    if rows:
        sb.table("coinapi_funding").upsert(rows).execute()
        print(f"[CoinAPI Funding] Upserted {len(rows)} rows")

if __name__ == "__main__":
    while True:
        try:
            ingest_open_interest()
            ingest_funding()
        except Exception as e:
            print("[error]", e)
        time.sleep(300)  # every 5 minutes

