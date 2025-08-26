import os
import time
import requests
from datetime import datetime, timezone
from supabase import create_client, Client

# ========= ENV VARS (Railway) =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
LIMIT_SYMBOLS = int(os.getenv("LIMIT_SYMBOLS", "50"))  # default: top 50

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

COINGECKO_API = "https://api.coingecko.com/api/v3/coins/markets"

def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def fetch_market_data():
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": LIMIT_SYMBOLS,
        "page": 1,
        "sparkline": "false"
    }

    r = requests.get(COINGECKO_API, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    rows = []
    for d in data:
        try:
            rows.append({
                "ts": iso_now(),
                "symbol": d["symbol"].upper() + "USDT",   # match Binance-style symbols (BTC → BTCUSDT)
                "price": float(d["current_price"]),
                "volume_24h": float(d["total_volume"]),
                "price_change_24h": float(d.get("price_change_percentage_24h") or 0.0),
                "market_cap": float(d["market_cap"]) if d["market_cap"] else None,
            })
        except Exception as e:
            print(f"[error] {d.get('id')}: {e}")
            continue
    return rows

def upsert_market_data(rows):
    if not rows:
        return
    print(f"[upsert] {len(rows)} Coingecko market rows…")
    sb.table("market_data").upsert(rows).execute()

def main():
    print("Starting Coingecko Market Data Job…")
    rows = fetch_market_data()
    upsert_market_data(rows)
    print("Done.")

if __name__ == "__main__":
    main()
