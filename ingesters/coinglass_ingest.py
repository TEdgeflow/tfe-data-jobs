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

def fetch_json(endpoint, params=None):
    url = f"{BASE_URL}{endpoint}"
    r = requests.get(url, headers=HEADERS, params=params)
    if r.status_code == 403:
        raise RuntimeError("403 Forbidden – check CoinGlass plan or key permissions.")
    r.raise_for_status()
    return r.json()

# ========= OI Aggregated =========
def ingest_oi_agg():
    data = fetch_json("/openInterest/ohlc-aggregated-history", params={"interval": "1h"})
    rows = []
    for d in data.get("data", []):
        ts = datetime.fromtimestamp(d.get("t") / 1000, tz=timezone.utc)
        rows.append({
            "symbol": d.get("symbol"),
            "exchange": d.get("exchangeName"),
            "oi": d.get("openInterestUsd"),
            "interval": "1h",
            "ts": ts
        })
    if rows:
        sb.table("derivatives_oi_agg").upsert(rows).execute()
        print(f"[CoinGlass OI Aggregated] Upserted {len(rows)} rows")

# ========= Funding Aggregated =========
def ingest_funding_agg():
    # OI-weighted
    data = fetch_json("/fundingRate/oi-weight-ohlc-history", params={"interval": "1h"})
    rows = []
    for d in data.get("data", []):
        ts = datetime.fromtimestamp(d.get("t") / 1000, tz=timezone.utc)
        rows.append({
            "symbol": d.get("symbol"),
            "funding_rate": d.get("fundingRate"),
            "weight_type": "oi_weighted",
            "interval": "1h",
            "ts": ts
        })
    if rows:
        sb.table("derivatives_funding_agg").upsert(rows).execute()
        print(f"[CoinGlass Funding OI-weighted] Upserted {len(rows)} rows")

    # Volume-weighted
    data = fetch_json("/fundingRate/vol-weight-ohlc-history", params={"interval": "1h"})
    rows = []
    for d in data.get("data", []):
        ts = datetime.fromtimestamp(d.get("t") / 1000, tz=timezone.utc)
        rows.append({
            "symbol": d.get("symbol"),
            "funding_rate": d.get("fundingRate"),
            "weight_type": "vol_weighted",
            "interval": "1h",
            "ts": ts
        })
    if rows:
        sb.table("derivatives_funding_agg").upsert(rows).execute()
        print(f"[CoinGlass Funding Vol-weighted] Upserted {len(rows)} rows")

# ========= Long/Short Ratios =========
def ingest_longshort():
    data = fetch_json("/longShort/global-history")
    rows = []
    for d in data.get("data", []):
        ts = datetime.fromtimestamp(d.get("t") / 1000, tz=timezone.utc)
        rows.append({
            "symbol": d.get("symbol"),
            "ratio_type": "global",
            "long_ratio": d.get("longAccount"),
            "short_ratio": d.get("shortAccount"),
            "ts": ts
        })
    if rows:
        sb.table("derivatives_longshort").upsert(rows).execute()
        print(f"[CoinGlass Long/Short Global] Upserted {len(rows)} rows")

    data = fetch_json("/longShort/topTrader-history")
    rows = []
    for d in data.get("data", []):
        ts = datetime.fromtimestamp(d.get("t") / 1000, tz=timezone.utc)
        rows.append({
            "symbol": d.get("symbol"),
            "ratio_type": "top_trader",
            "long_ratio": d.get("longAccount"),
            "short_ratio": d.get("shortAccount"),
            "ts": ts
        })
    if rows:
        sb.table("derivatives_longshort").upsert(rows).execute()
        print(f"[CoinGlass Long/Short Top Trader] Upserted {len(rows)} rows")

# ========= Options Max Pain =========
def ingest_options():
    url = "https://open-api.coinglass.com/api/options/max-pain"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    data = r.json()

    rows = []
    for d in data.get("data", []):
        ts = iso_now()
        rows.append({
            "symbol": d.get("symbol"),
            "max_pain_price": d.get("maxPain"),
            "ts": ts
        })
    if rows:
        sb.table("derivatives_options").upsert(rows).execute()
        print(f"[CoinGlass Options Max Pain] Upserted {len(rows)} rows")

# ========= MAIN LOOP =========
if __name__ == "__main__":
    while True:
        try:
            ingest_oi_agg()
            ingest_funding_agg()
            ingest_longshort()
            ingest_options()
        except Exception as e:
            print("[error]", e)
        time.sleep(600)  # run every 10 minutes




