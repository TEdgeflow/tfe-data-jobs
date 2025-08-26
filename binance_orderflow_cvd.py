import os
import time
import json
import requests
from datetime import datetime, timezone
from supabase import create_client, Client

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BINANCE_REST = "https://fapi.binance.com/fapi/v1/aggTrades"

# Keep a running CVD
cumulative_cvd = {}

def fetch_trades(symbol="BTCUSDT", limit=500):
    """Fetch aggregated trades from Binance futures (REST fallback)."""
    url = BINANCE_REST
    params = {"symbol": symbol, "limit": limit}
    r = requests.get(url, params=params)
    r.raise_for_status()
    return r.json()

def process_trades(symbol, trades):
    """Convert Binance trades into orderflow metrics."""
    global cumulative_cvd

    buys, sells = 0.0, 0.0
    for t in trades:
        qty = float(t["q"])   # quantity
        price = float(t["p"])
        is_buyer_maker = t["m"]  # True if SELLER is taker → means SELL trade
        if is_buyer_maker:
            sells += qty
        else:
            buys += qty

    delta = buys - sells
    prev_cvd = cumulative_cvd.get(symbol, 0.0)
    cvd = prev_cvd + delta
    cumulative_cvd[symbol] = cvd

    row = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "symbol": symbol,
        "buys_volume": buys,
        "sells_volume": sells,
        "delta": delta,
        "cvd": cvd,
    }
    return row

def upsert_orderflow(row):
    """Insert orderflow row into Supabase."""
    try:
        sb.table("orderflow_cvd").upsert(row).execute()
        print(f"[upsert] {row['symbol']} Δ={row['delta']:.2f} CVD={row['cvd']:.2f}")
    except Exception as e:
        print("[error] Supabase insert failed:", e)

def main():
    symbols = ["BTCUSDT", "ETHUSDT"]  # expand later
    while True:
        try:
            for sym in symbols:
                trades = fetch_trades(sym, limit=200)
                row = process_trades(sym, trades)
                upsert_orderflow(row)
            time.sleep(30)  # every 30s
        except Exception as e:
            print("[error]", e)
            time.sleep(10)

if __name__ == "__main__":
    print("[debug] Starting Binance Orderflow CVD ingestion")
    print("[debug] SUPABASE_URL present?", bool(SUPABASE_URL))
    print("[debug] SUPABASE_KEY present?", bool(SUPABASE_KEY))
    main()
