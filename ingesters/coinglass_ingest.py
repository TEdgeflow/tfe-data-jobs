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
    "SUPABASE_URL": SUPABASE_URL,
    "SUPABASE_KEY": SUPABASE_KEY,
    "COINGLASS_API_KEY": COINGLASS_API_KEY
})
if not SUPABASE_URL or not SUPABASE_KEY or not COINGLASS_API_KEY:
    raise RuntimeError("Missing SUPABASE_URL, SUPABASE_KEY, or COINGLASS_API_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE_URL = "https://open-api-v4.coinglass.com/api"
HEADERS = {"accept": "application/json", "CG-API-KEY": COINGLASS_API_KEY}

# ========= HELPERS =========
def fetch_json(endpoint, params=None):
    url = f"{BASE_URL}{endpoint}"
    r = requests.get(url, headers=HEADERS, params=params or {})
    if r.status_code != 200:
        print(f"[error] {endpoint} {r.status_code} {r.text}")
        return None
    try:
        return r.json()
    except Exception:
        print(f"[error] invalid json for {endpoint}")
        return None

def upsert(table, rows):
    if not rows:
        return
    try:
        sb.table(table).upsert(rows).execute()
        print(f"[ok] {len(rows)} rows -> {table}")
    except Exception as e:
        print(f"[error] upsert {table}: {e}")

# ========= INGESTIONS =========

def ingest_oi():
    """Open Interest OHLC aggregated history"""
    data = fetch_json("/futures/openInterest/ohlc-aggregated-history", {"interval": "1h"})
    if not data or "data" not in data:
        return
    rows = []
    for item in data["data"]:
        rows.append({
            "symbol": item.get("symbol"),
            "oi": item.get("openInterest"),
            "timestamp": datetime.fromtimestamp(item["timestamp"]/1000, tz=timezone.utc).isoformat()
        })
    upsert("derivatives_oi", rows)

def ingest_funding():
    """Funding Rate history"""
    data = fetch_json("/futures/fundingRate/oi-weight-ohlc-history", {"interval": "1h"})
    if not data or "data" not in data:
        return
    rows = []
    for item in data["data"]:
        rows.append({
            "symbol": item.get("symbol"),
            "funding_rate": item.get("fundingRate"),
            "timestamp": datetime.fromtimestamp(item["timestamp"]/1000, tz=timezone.utc).isoformat()
        })
    upsert("derivatives_funding", rows)

def ingest_liquidations():
    """Liquidation history"""
    data = fetch_json("/futures/liquidation/history", {"interval": "1h"})
    if not data or "data" not in data:
        return
    rows = []
    for item in data["data"]:
        rows.append({
            "symbol": item.get("symbol"),
            "side": item.get("side"),  # long / short
            "amount": item.get("amount"),
            "price": item.get("price"),
            "timestamp": datetime.fromtimestamp(item["timestamp"]/1000, tz=timezone.utc).isoformat()
        })
    upsert("derivatives_liquidations", rows)

def ingest_taker_volume():
    """Taker Buy/Sell Volume"""
    data = fetch_json("/futures/taker-buy-sell-volume/history", {"interval": "1h"})
    if not data or "data" not in data:
        return
    rows = []
    for item in data["data"]:
        rows.append({
            "symbol": item.get("symbol"),
            "buy_volume": item.get("buyVol"),
            "sell_volume": item.get("sellVol"),
            "timestamp": datetime.fromtimestamp(item["timestamp"]/1000, tz=timezone.utc).isoformat()
        })
    upsert("derivatives_taker_volume", rows)

# ========= MAIN =========
def run_all():
    print("🚀 Starting CoinGlass ingestion...")
    ingest_oi()
    ingest_funding()
    ingest_liquidations()
    ingest_taker_volume()
    print("✅ Done.")

if __name__ == "__main__":
    while True:
        run_all()
        time.sleep(3600)  # run every 1h












