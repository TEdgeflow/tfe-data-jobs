import os
import time
import requests
from datetime import datetime
from supabase import create_client, Client

# ===== Supabase setup =====
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ===== Config =====
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]  # expand as needed
BINANCE_URL = "https://api.binance.com/api/v3/trades"

def fetch_trades(symbol, limit=1000):
    url = f"{BINANCE_URL}?symbol={symbol}&limit={limit}"
    r = requests.get(url)
    r.raise_for_status()
    return r.json()

def ingest_trades():
    for symbol in SYMBOLS:
        trades = fetch_trades(symbol)
        rows = []
        for t in trades:
            row = {
                "symbol": symbol,
                "trade_id": t["id"],
                "price": float(t["price"]),
                "qty": float(t["qty"]),
                "quote_qty": float(t["quoteQty"]),
                "side": "BUY" if not t["isBuyerMaker"] else "SELL",
                "is_buyer_maker": t["isBuyerMaker"],
                "ts": datetime.fromtimestamp(t["time"]/1000.0)
            }
            rows.append(row)

        if rows:
            sb.table("binance_trades").upsert(rows).execute()
            print(f"[{symbol}] Inserted {len(rows)} trades")

if __name__ == "__main__":
    while True:
        try:
            ingest_trades()
        except Exception as e:
            print("Error:", e)
        time.sleep(60)   # fetch every 1 minute
