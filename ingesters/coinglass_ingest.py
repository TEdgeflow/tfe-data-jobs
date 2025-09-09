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
BASE_URL = "https://open-api-v4.coinglass.com/api"


# ========= HELPERS =========
def iso_now(ts_ms=None):
    if ts_ms:
        return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()
    return datetime.now(timezone.utc).isoformat()

def fetch_json(path, params=None):
    url = f"{BASE_URL}{path}"
    r = requests.get(url, headers=HEADERS, params=params)
    if r.status_code == 403:
        raise RuntimeError("403 Forbidden – check CoinGlass plan or key permissions.")
    r.raise_for_status()
    return r.json()


# ========= INGEST FUNCTIONS =========
def ingest_open_interest():
    data = fetch_json("/futures/openInterest/ohlc-history", {
        "symbol": "BTC",   # ⚡ later: loop over BTC, ETH, SOL etc
        "interval": "1h"
    })

    rows = []
    for d in data.get("data", []):
        rows.append({
            "symbol": "BTC",
            "oi": d.get("openInterestUsd"),
            "timestamp": iso_now(d.get("time"))
        })

    if rows:
        sb.table("derivatives_oi").upsert(rows).execute()
        print(f"[OI] Upserted {len(rows)} rows")


def ingest_funding():
    data = fetch_json("/futures/fundingRate/ohlc-history", {
        "symbol": "BTC",
        "interval": "1h"
    })

    rows = []
    for d in data.get("data", []):
        rows.append({
            "symbol": "BTC",
            "funding_rate": d.get("fundingRate"),
            "timestamp": iso_now(d.get("time"))
        })

    if rows:
        sb.table("derivatives_funding").upsert(rows).execute()
        print(f"[Funding] Upserted {len(rows)} rows")


def ingest_liquidations():
    data = fetch_json("/futures/liquidation/history", {
        "symbol": "BTC",
        "interval": "1h"
    })

    rows = []
    for d in data.get("data", []):
        rows.append({
            "symbol": "BTC",
            "side": d.get("side"),
            "amount": d.get("amount"),
            "price": d.get("price"),
            "timestamp": iso_now(d.get("time"))
        })

    if rows:
        sb.table("derivatives_liquidations").upsert(rows).execute()
        print(f"[Liquidations] Upserted {len(rows)} rows")


def ingest_taker_volume():
    data = fetch_json("/futures/taker-buy-sell-volume/history", {
        "symbol": "BTC",
        "interval": "1h"
    })

    rows = []
    for d in data.get("data", []):
        rows.append({
            "symbol": "BTC",
            "buy_volume": d.get("buyVol"),
            "sell_volume": d.get("sellVol"),
            "timestamp": iso_now(d.get("time"))
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








