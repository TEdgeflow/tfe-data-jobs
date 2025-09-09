import os
import time
import requests
from datetime import datetime, timezone
from supabase import create_client, Client

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
COINGLASS_KEY = os.getenv("COINGLASS_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing Supabase credentials")
if not COINGLASS_KEY:
    raise RuntimeError("Missing COINGLASS_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

HEADERS = {
    "CG-API-KEY": COINGLASS_KEY,
    "accept": "application/json"
}
BASE_URL = "https://open-api-v4.coinglass.com/api/futures"

# ========= HELPERS =========
def iso_now():
    return datetime.now(timezone.utc).isoformat()

def fetch_json(url, params=None):
    r = requests.get(url, headers=HEADERS, params=params)
    if r.status_code == 403:
        raise RuntimeError("403 Forbidden – check CoinGlass plan or key permissions.")
    if r.status_code == 404:
        raise RuntimeError(f"404 Not Found – check endpoint {url}")
    r.raise_for_status()
    return r.json()

# ========= INGEST OI =========
def ingest_open_interest():
    url = f"{BASE_URL}/openInterest/ohlc-history"
    data = fetch_json(url)

    rows = []
    for d in data.get("data", []):
        symbol = d.get("symbol")
        exchange = d.get("exchangeName")
        oi = d.get("openInterestUsd")

        rows.append({
            "symbol": symbol,
            "exchange": exchange,
            "oi": oi,
            "timestamp": iso_now()
        })

    if rows:
        sb.table("derivatives_oi").upsert(rows).execute()
        print(f"[CoinGlass OI] Upserted {len(rows)} rows")

# ========= INGEST FUNDING =========
def ingest_funding():
    url = f"{BASE_URL}/fundingRate/ohlc-history"
    data = fetch_json(url)

    rows = []
    for d in data.get("data", []):
        symbol = d.get("symbol")
        exchange = d.get("exchangeName")
        fr = d.get("fundingRate")

        rows.append({
            "symbol": symbol,
            "exchange": exchange,
            "funding_rate": fr,
            "timestamp": iso_now()
        })

    if rows:
        sb.table("derivatives_funding").upsert(rows).execute()
        print(f"[CoinGlass Funding] Upserted {len(rows)} rows")

# ========= MAIN LOOP =========
if __name__ == "__main__":
    while True:
        try:
            ingest_open_interest()
            ingest_funding()
        except Exception as e:
            print("[error]", e)
        time.sleep(300)  # every 5 minutes





