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

BASE_URL = "https://open-api-v4.coinglass.com/api"
HEADERS = {
    "CG-API-KEY": COINGLASS_KEY,
    "accept": "application/json"
}

# ========= HELPERS =========
def iso_now(ms=None):
    if ms:
        return datetime.fromtimestamp(ms/1000, tz=timezone.utc).isoformat()
    return datetime.now(timezone.utc).isoformat()

def fetch_json(endpoint, params=None):
    url = f"{BASE_URL}{endpoint}"
    r = requests.get(url, headers=HEADERS, params=params)
    if r.status_code != 200:
        print("[error]", r.status_code, url, r.text[:200])
        return {}
    return r.json()

# ========= SUPPORTED COINS =========
def get_supported_coins():
    data = fetch_json("/futures/supported-coins")
    return [c.get("symbol") for c in data.get("data", []) if c.get("symbol")]

# ========= INGEST OI =========
def ingest_open_interest(symbols):
    for sym in symbols:
        data = fetch_json("/futures/openInterest/ohlc-history", {
            "symbol": sym,
            "interval": "1h"
        })
        rows = []
        for d in data.get("data", []):
            rows.append({
                "symbol": sym,
                "oi": d.get("openInterestUsd"),
                "timestamp": iso_now(d.get("time"))
            })
        if rows:
            sb.table("derivatives_oi").upsert(rows).execute()
            print(f"[OI] {sym} → {len(rows)} rows")

# ========= INGEST FUNDING =========
def ingest_funding(symbols):
    for sym in symbols:
        data = fetch_json("/futures/fundingRate/ohlc-history", {
            "symbol": sym,
            "interval": "1h"
        })
        rows = []
        for d in data.get("data", []):
            rows.append({
                "symbol": sym,
                "funding_rate": d.get("fundingRate"),
                "timestamp": iso_now(d.get("time"))
            })
        if rows:
            sb.table("derivatives_funding").upsert(rows).execute()
            print(f"[Funding] {sym} → {len(rows)} rows")

# ========= INGEST LIQUIDATIONS =========
def ingest_liquidations(symbols):
    for sym in symbols:
        data = fetch_json("/futures/liquidation/history", {
            "symbol": sym,
            "interval": "1h"
        })
        rows = []
        for d in data.get("data", []):
            rows.append({
                "symbol": sym,
                "side": d.get("side"),  # buy/sell
                "volume_usd": d.get("volUsd"),
                "timestamp": iso_now(d.get("time"))
            })
        if rows:
            sb.table("derivatives_liquidations").upsert(rows).execute()
            print(f"[Liquidations] {sym} → {len(rows)} rows")

# ========= INGEST TAKER BUY/SELL =========
def ingest_taker_volume(symbols):
    for sym in symbols:
        data = fetch_json("/futures/taker-buy-sell-volume/history", {
            "symbol": sym,
            "interval": "1h"
        })
        rows = []
        for d in data.get("data", []):
            rows.append({
                "symbol": sym,
                "buy_vol": d.get("buyVolUsd"),
                "sell_vol": d.get("sellVolUsd"),
                "timestamp": iso_now(d.get("time"))
            })
        if rows:
            sb.table("derivatives_taker_volume").upsert(rows).execute()
            print(f"[TakerVolume] {sym} → {len(rows)} rows")

# ========= MAIN =========
if __name__ == "__main__":
    while True:
        try:
            symbols = get_supported_coins()[:10]  # ⚠️ LIMIT to first 10 for free tier
            print("Pulling for:", symbols)

            ingest_open_interest(symbols)
            ingest_funding(symbols)
            ingest_liquidations(symbols)
            ingest_taker_volume(symbols)

        except Exception as e:
            print("[error]", e)

        time.sleep(600)  # every 10 min









