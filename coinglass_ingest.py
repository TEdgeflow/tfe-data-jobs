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

# ========= HELPERS =========
def iso_now():
    return datetime.now(timezone.utc).isoformat()

def fetch_json(url, params=None):
    r = requests.get(url, headers=HEADERS, params=params)
    if r.status_code == 403:
        raise RuntimeError("403 Forbidden – check CoinGlass plan or key permissions.")
    r.raise_for_status()
    return r.json()

def parse_symbol(pair: str):
    """
    CoinGlass returns pairs like BTCUSDT, ETHUSDT, etc.
    """
    if pair.endswith("USDT"):
        return pair[:-4], "USDT"
    elif pair.endswith("USD"):
        return pair[:-3], "USD"
    else:
        return pair, None

# ========= INGEST OI =========
def ingest_open_interest():
    url = f"{BASE_URL}/openInterest"
    data = fetch_json(url)

    rows = []
    for d in data.get("data", []):
        pair = d.get("symbol")
        base, quote = parse_symbol(pair)
        exchange = d.get("exchangeName")
        oi = d.get("openInterestUsd")

        rows.append({
            "symbol": pair,
            "base_asset": base,
            "quote_asset": quote,
            "exchange": exchange,
            "oi": oi,
            "timestamp": iso_now()
        })

    if rows:
        sb.table("derivatives_oi").upsert(rows).execute()
        print(f"[CoinGlass OI] Upserted {len(rows)} rows")

# ========= INGEST FUNDING =========
def ingest_funding():
    url = f"{BASE_URL}/fundingRate"
    data = fetch_json(url)

    rows = []
    for d in data.get("data", []):
        pair = d.get("symbol")
        base, quote = parse_symbol(pair)
        exchange = d.get("exchangeName")
        fr = d.get("fundingRate")

        rows.append({
            "symbol": pair,
            "base_asset": base,
            "quote_asset": quote,
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
