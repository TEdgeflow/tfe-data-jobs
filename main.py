import os
import time
from datetime import datetime, timezone
import requests
from supabase import create_client, Client

# ========= ENV VARS (from Railway) =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Symbols to track
SYMBOLS = ["BTCUSDT", "ETHUSDT"]
VENUE = "binance"

def iso_from_ms(ms: int) -> str:
    return datetime.fromtimestamp(ms/1000, tz=timezone.utc).isoformat()

def fetch_funding(symbol: str):
    url = "https://fapi.binance.com/fapi/v1/fundingRate"
    r = requests.get(url, params={"symbol": symbol, "limit": 1}, timeout=10)
    r.raise_for_status()
    data = r.json()
    if not data:
        return None
    item = data[-1]
    return {
        "ts": iso_from_ms(int(item["fundingTime"])),
        "venue": VENUE,
        "symbol": symbol,
        "rate": float(item["fundingRate"])
    }

def fetch_open_interest(symbol: str):
    # 5-minute period OI history; we take the latest point
    url = "https://fapi.binance.com/futures/data/openInterestHist"
    r = requests.get(url, params={"symbol": symbol, "period": "5m", "limit": 1}, timeout=10)
    r.raise_for_status()
    data = r.json()
    if not data:
        return None
    item = data[-1]
    # sumOpenInterest comes as string number
    oi = float(item["sumOpenInterest"])
    ts = iso_from_ms(int(item["timestamp"]))
    return {
        "ts": ts,
        "venue": VENUE,
        "symbol": symbol,
        "oi_usd": oi
    }

def upsert(table: str, rows: list, conflict_cols: list):
    if not rows:
        return
    # Supabase Python client supports upsert with on_conflict
    sb.table(table).upsert(rows, on_conflict=",".join(conflict_cols)).execute()

def run():
    funding_rows = []
    oi_rows = []

    for sym in SYMBOLS:
        try:
            f = fetch_funding(sym)
            if f: funding_rows.append(f)
        except Exception as e:
            print(f"[funding] {sym} error: {e}")

        try:
            oi = fetch_open_interest(sym)
            if oi: oi_rows.append(oi)
        except Exception as e:
            print(f"[oi] {sym} error: {e}")

        time.sleep(0.2)  # be nice to API

    if funding_rows:
        print(f"Upserting {len(funding_rows)} funding rows…")
        upsert("funding_rates", funding_rows, ["ts","venue","symbol"])
    if oi_rows:
        print(f"Upserting {len(oi_rows)} OI rows…")
        upsert("open_interest", oi_rows, ["ts","venue","symbol"])

    print("Done.")

if __name__ == "__main__":
    run()
