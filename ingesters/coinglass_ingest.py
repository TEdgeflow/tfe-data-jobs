import os
import time
import requests
from datetime import datetime, timezone
from supabase import create_client, Client

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
COINGLASS_API_KEY = os.getenv("COINGLASS_API_KEY")

print("DEBUG ENV:", {
    "SUPABASE_URL": SUPABASE_URL if SUPABASE_URL else "❌ MISSING",
    "SUPABASE_KEY": (SUPABASE_KEY[:6] + "..." if SUPABASE_KEY else "❌ MISSING"),
    "COINGLASS_API_KEY": (COINGLASS_API_KEY[:6] + "..." if COINGLASS_API_KEY else "❌ MISSING")
})

if not SUPABASE_URL or not SUPABASE_KEY or not COINGLASS_API_KEY:
    print("⚠️ WARNING: Missing one or more ENV vars, skipping ingestion.")
    exit(0)  # Stop here so logs show missing vars without spamming

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

HEADERS = {
    "CG-API-KEY": COINGLASS_API_KEY,
    "accept": "application/json"
}
BASE_URL = "https://open-api-v4.coinglass.com/api"

# ========= HELPERS =========
def iso_now():
    return datetime.now(timezone.utc).isoformat()

def fetch_json(path, params=None):
    url = f"{BASE_URL}{path}"
    r = requests.get(url, headers=HEADERS, params=params)
    if r.status_code != 200:
        print(f"[error] {r.status_code} {url} -> {r.text}")
        return {}
    return r.json()

# ========= INGEST FUNCTIONS =========
def ingest_open_interest():
    data = fetch_json("/futures/openInterest/ohlc-aggregated-history", {"interval": "1h"})
    rows = []
    for d in data.get("data", []):
        rows.append({
            "symbol": d.get("symbol"),
            "oi": d.get("openInterest"),
            "timestamp": iso_now()
        })
    if rows:
        sb.table("derivatives_oi").upsert(rows).execute()
        print(f"[OI] Upserted {len(rows)} rows")

def ingest_funding():
    data = fetch_json("/futures/fundingRate/oi-weight-ohlc-history", {"interval": "1h"})
    rows = []
    for d in data.get("data", []):
        rows.append({
            "symbol": d.get("symbol"),
            "funding_rate": d.get("fundingRate"),
            "timestamp": iso_now()
        })
    if rows:
        sb.table("derivatives_funding").upsert(rows).execute()
        print(f"[Funding] Upserted {len(rows)} rows")

def ingest_liquidations():
    data = fetch_json("/futures/liquidation/aggregated-history", {"interval": "1h"})
    rows = []
    for d in data.get("data", []):
        rows.append({
            "symbol": d.get("symbol"),
            "amount": d.get("amount"),
            "price": d.get("price"),
            "side": d.get("side"),
            "timestamp": iso_now()
        })
    if rows:
        sb.table("derivatives_liquidations").upsert(rows).execute()
        print(f"[Liquidations] Upserted {len(rows)} rows")

def ingest_taker_volume():
    data = fetch_json("/futures/aggregated-taker-buy-sell-volume/history", {"interval": "1h"})
    rows = []
    for d in data.get("data", []):
        rows.append({
            "symbol": d.get("symbol"),
            "buy_volume": d.get("buyVol"),
            "sell_volume": d.get("sellVol"),
            "timestamp": iso_now()
        })
    if rows:
        sb.table("derivatives_taker_volume").upsert(rows).execute()
        print(f"[Taker Volume] Upserted {len(rows)} rows")

# ========= MAIN LOOP =========
if __name__ == "__main__":
    while True:
        try:
            ingest_open_interest()
            ingest_funding()
            ingest_liquidations()
            ingest_taker_volume()
        except Exception as e:
            print("[error]", e)
        time.sleep(300)  # every 5 min













