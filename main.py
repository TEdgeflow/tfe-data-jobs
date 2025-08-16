import os
import time
from datetime import datetime, timezone
import requests
from supabase import create_client, Client

# ========= ENV VARS (Railway) =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
LIMIT_SYMBOLS = int(os.getenv("LIMIT_SYMBOLS", "0"))

print("[boot] HAS_URL=", bool(SUPABASE_URL), "HAS_KEY=", bool(SUPABASE_KEY))
if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

VENUE = "binance"
BINANCE_FAPI = "https://fapi.binance.com"

# Convert milliseconds to ISO timestamp
def iso_from_ms(ms: int) -> str:
    return datetime.fromtimestamp(ms/1000, tz=timezone.utc).isoformat()

# Get all USDT perpetual pairs
def get_perp_symbols_usdt():
    url = f"{BINANCE_FAPI}/fapi/v1/exchangeInfo"
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    info = r.json()
    syms = []
    for s in info.get("symbols", []):
        if (
            s.get("contractType") == "PERPETUAL"
            and s.get("quoteAsset") == "USDT"
            and s.get("status") == "TRADING"
        ):
            syms.append(s["symbol"])
    syms = sorted(set(syms))
    if LIMIT_SYMBOLS and LIMIT_SYMBOLS > 0:
        syms = syms[:LIMIT_SYMBOLS]
    print(f"[symbols] Found {len(syms)} USDT-PERP symbols.")
    return syms

# Fetch funding rate
def fetch_funding(symbol: str):
    url = f"{BINANCE_FAPI}/fapi/v1/fundingRate"
    r = requests.get(url, params={"symbol": symbol, "limit": 1}, timeout=10)
    r.raise_for_status()
    data = r.json()
    if not data:
        return None
    item = data[-1]
    return {
        "funding_time": iso_from_ms(int(item["fundingTime"])),
        "venue": VENUE,
        "symbol": symbol,
        "funding_rate": float(item["fundingRate"]),
    }

# Fetch open interest
def fetch_open_interest(symbol: str):
    url = f"{BINANCE_FAPI}/futures/data/openInterestHist"
    r = requests.get(url, params={"symbol": symbol, "period": "5m", "limit": 1}, timeout=10)
    r.raise_for_status()
    data = r.json()
    if not data:
        return None
    item = data[-1]
    ts = iso_from_ms(int(item["timestamp"]))
    try:
        oi_usd = float(item.get("sumOpenInterestValue")) if item.get("sumOpenInterestValue") is not None else None
    except Exception:
        oi_usd = None
    if oi_usd is None:
        try:
            oi_usd = float(item.get("sumOpenInterest"))
        except Exception:
            oi_usd = 0.0
    return {
        # ✅ match DB: oi_time + open_interest
        "oi_time": ts,
        "venue": VENUE,
        "symbol": symbol,
        "open_interest": oi_usd,
    }

# Generic upsert
def upsert(table: str, rows: list, conflict_cols: list):
    if not rows:
        return
    sb.table(table).upsert(rows, on_conflict=",".join(conflict_cols)).execute()

def run():
    symbols = get_perp_symbols_usdt()

    funding_rows = []
    oi_rows = []

    for i, sym in enumerate(symbols, start=1):
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

        time.sleep(0.06)

        # Batch upsert every 50 rows
        if i % 50 == 0:
            if funding_rows:
                print(f"Upserting {len(funding_rows)} funding rows (batch)…")
                upsert("funding_rates", funding_rows, ["funding_time","venue","symbol"])
                funding_rows.clear()
            if oi_rows:
                print(f"Upserting {len(oi_rows)} OI rows (batch)…")
                upsert("open_interest", oi_rows, ["oi_time","venue","symbol"])
                oi_rows.clear()

    # Final flush
    if funding_rows:
        print(f"Upserting {len(funding_rows)} funding rows (final)…")
        upsert("funding_rates", funding_rows, ["funding_time","venue","symbol"])
    if oi_rows:
        print(f"Upserting {len(oi_rows)} OI rows (final)…")
        upsert("open_interest", oi_rows, ["oi_time","venue","symbol"])

    print("Done.")

if __name__ == "__main__":
    run()

