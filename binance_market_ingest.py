import os
import time
import requests
from datetime import datetime, timezone
from supabase import create_client, Client

# ========= ENV VARS (Railway) =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
LIMIT_SYMBOLS = int(os.getenv("LIMIT_SYMBOLS", "0"))

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BINANCE_FAPI = "https://fapi.binance.com"

def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def get_perp_symbols():
    r = requests.get(f"{BINANCE_FAPI}/fapi/v1/exchangeInfo", timeout=10)
    r.raise_for_status()
    data = r.json()
    symbols = [s["symbol"] for s in data["symbols"] if s["quoteAsset"] == "USDT" and s["contractType"] == "PERPETUAL"]
    if LIMIT_SYMBOLS > 0:
        symbols = symbols[:LIMIT_SYMBOLS]
    print(f"[symbols] Found {len(symbols)} USDT-PERP symbols.")
    return symbols

def fetch_market_data(symbols):
    rows = []
    for sym in symbols:
        try:
            r = requests.get(f"{BINANCE_FAPI}/fapi/v1/ticker/24hr?symbol={sym}", timeout=10)
            r.raise_for_status()
            d = r.json()
            rows.append({
                "ts": iso_now(),
                "symbol": sym,
                "price": float(d["lastPrice"]),
                "volume_24h": float(d["quoteVolume"]),          # ✅ matches Supabase column
                "price_change_24h": float(d["priceChangePercent"]),  # ✅ matches Supabase column
            })
        except Exception as e:
            print(f"[error] {sym}: {e}")
            continue
        time.sleep(0.1)  # polite delay
    return rows

def upsert_market_data(rows):
    if not rows:
        return
    print(f"[upsert] {len(rows)} market rows…")
    sb.table("market_data").upsert(rows).execute()

def main():
    print("Starting Market Data Job…")
    symbols = get_perp_symbols()
    rows = fetch_market_data(symbols)
    upsert_market_data(rows)
    print("Done.")

if __name__ == "__main__":
    main()



