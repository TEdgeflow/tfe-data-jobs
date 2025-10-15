import os
import time
import requests
from datetime import datetime, timezone
from supabase import create_client, Client

# ---- Setup ----
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
BINANCE_API = "https://api.binance.com/api/v3"
sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ---- Fetch Binance Symbols ----
def get_all_usdt_symbols():
    url = f"{BINANCE_API}/exchangeInfo"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    data = r.json()
    return [
        s["symbol"]
        for s in data["symbols"]
        if s["quoteAsset"] == "USDT" and s["status"] == "TRADING"
    ]

# ---- Fetch Market Data ----
def fetch_market_data(symbols):
    rows = []
    ts = datetime.now(timezone.utc).isoformat()
    for sym in symbols:
        try:
            r = requests.get(f"{BINANCE_API}/ticker/24hr?symbol={sym}", timeout=10)
            r.raise_for_status()
            d = r.json()
            price = float(d["lastPrice"])
            volume_24h = float(d["quoteVolume"])
            price_change_24h = float(d["priceChangePercent"])
            market_cap_est = price * volume_24h  # proxy market cap

            rows.append({
                "ts": ts,
                "symbol": sym,
                "price": price,
                "volume_24h": volume_24h,
                "price_change_24h": price_change_24h,
                "market_cap_est": market_cap_est,
            })
        except Exception as e:
            print(f"[ERROR] {sym}: {e}")
            time.sleep(0.1)
            continue
    return rows

# ---- Upsert to Supabase ----
def upsert_market_data(rows):
    if not rows:
        return
    sb.table("binance_market_data").upsert(rows).execute()
    print(f"[UPSERT] {len(rows)} Binance market rows inserted/updated")

# ---- Main Loop ----
def main():
    while True:
        try:
            symbols = get_all_usdt_symbols()
            print(f"[INFO] Found {len(symbols)} USDT pairs")
            data = fetch_market_data(symbols)
            upsert_market_data(data)
            print(f"[DONE] Binance market data cycle completed.")
        except Exception as e:
            print(f"[FATAL] {e}")
        time.sleep(1800)  # run every 30 minutes

if __name__ == "__main__":
    main()

