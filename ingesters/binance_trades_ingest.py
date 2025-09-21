import os
import time
import requests
from datetime import datetime
from supabase import create_client, Client

# ===== Supabase setup =====
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BINANCE_URL = "https://api.binance.com/api/v3"

def get_all_usdt_symbols():
    """Fetch all active USDT pairs from Binance"""
    url = f"{BINANCE_URL}/exchangeInfo"
    r = requests.get(url)
    r.raise_for_status()
    data = r.json()
    return [
        s["symbol"] for s in data["symbols"]
        if s["quoteAsset"] == "USDT" and s["status"] == "TRADING"
    ]

def fetch_trades(symbol, limit=1000):
    url = f"{BINANCE_URL}/trades?symbol={symbol}&limit={limit}"
    r = requests.get(url)
    r.raise_for_status()
    return r.json()

def ingest_trades():
    symbols = get_all_usdt_symbols()
    print(f"[INFO] Found {len(symbols)} USDT pairs")

    for symbol in symbols:
        try:
            trades = fetch_trades(symbol)
            rows = []
            for t in trades:
                rows.append({
                    "symbol": symbol,
                    "trade_id": t["id"],
                    "price": float(t["price"]),
                    "qty": float(t["qty"]),
                    "quote_qty": float(t["quoteQty"]),
                    "side": "BUY" if not t["isBuyerMaker"] else "SELL",
                    "is_buyer_maker": t["isBuyerMaker"],
                    "ts": datetime.fromtimestamp(t["time"]/1000.0).isoformat()
                })
            if rows:
                sb.table("binance_trades").upsert(rows).execute()
                print(f"[{symbol}] Inserted {len(rows)} trades")
        except Exception as e:
            print(f"[ERROR] {symbol}: {e}")

if __name__ == "__main__":
    while True:
        try:
            ingest_trades()
        except Exception as e:
            print("Fatal error:", e)
        time.sleep(300)  # run every 5 minutes


