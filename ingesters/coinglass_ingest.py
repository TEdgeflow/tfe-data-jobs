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

HEADERS = {"coinglassSecret": COINGLASS_KEY}
BASE_URL = "https://open-api.coinglass.com/api/futures"

def iso_now():
    return datetime.now(timezone.utc).isoformat()

def fetch_json(url, params=None):
    r = requests.get(url, headers=HEADERS, params=params)
    if r.status_code == 403:
        raise RuntimeError("403 Forbidden – check CoinGlass plan or key permissions.")
    r.raise_for_status()
    return r.json()

# ========= Get Supported Symbols =========
def get_supported_symbols():
    url = f"{BASE_URL}/symbols"
    data = fetch_json(url)
    return [d.get("symbol") for d in data.get("data", []) if d.get("symbol")]

# ========= Ingest OI =========
def ingest_open_interest(symbols):
    url = f"{BASE_URL}/openInterest"
    rows = []

    for sym in symbols:
        try:
            data = fetch_json(url, params={"symbol": sym})
            for d in data.get("data", []):
                rows.append({
                    "symbol": d.get("symbol"),
                    "exchange": d.get("exchangeName"),
                    "oi": d.get("openInterestUsd"),
                    "timestamp": iso_now()
                })
        except Exception as e:
            print(f"[OI error] {sym}:", e)

    if rows:
        sb.table("derivatives_oi").upsert(rows).execute()
        print(f"[CoinGlass OI] Upserted {len(rows)} rows")

# ========= Ingest Funding =========
def ingest_funding(symbols):
    url = f"{BASE_URL}/fundingRate"
    rows = []

    for sym in symbols:
        try:
            data = fetch_json(url, params={"symbol": sym})
            for d in data.get("data", []):
                rows.append({
                    "symbol": d.get("symbol"),
                    "exchange": d.get("exchangeName"),
                    "funding_rate": d.get("fundingRate"),
                    "timestamp": iso_now()
                })
        except Exception as e:
            print(f"[FR error] {sym}:", e)

    if rows:
        sb.table("derivatives_funding").upsert(rows).execute()
        print(f"[CoinGlass Funding] Upserted {len(rows)} rows")

# ========= Main Loop =========
if __name__ == "__main__":
    while True:
        try:
            symbols = get_supported_symbols()
            print(f"[Symbols] Fetched {len(symbols)} supported symbols")

            ingest_open_interest(symbols)
            ingest_funding(symbols)

        except Exception as e:
            print("[error]", e)
        time.sleep(600)  # every 10 min

