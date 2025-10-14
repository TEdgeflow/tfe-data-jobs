import os
import time
import requests
from datetime import datetime, timedelta, timezone
from supabase import create_client, Client

# ===== Supabase setup =====
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BINANCE_URL = "https://api.binance.com/api/v3"

def get_all_usdt_symbols():
    """Fetch all active USDT pairs from Binance"""
    url = f"{BINANCE_URL}/exchangeInfo"
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    data = r.json()
    return [
        s["symbol"] for s in data["symbols"]
        if s["quoteAsset"] == "USDT" and s["status"] == "TRADING"
    ]

def fetch_trades(symbol, limit=1000):
    """Fetch latest Binance trades 24h for a given symbol."""
    url = f"{BINANCE_URL}/trades"
    r = requests.get(url, params={"symbol": symbol, "limit": limit}, timeout=10)
    r.raise_for_status()
    return r.json()

def cleanup_old_rows():
    """Delete rows older than 24 hours."""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=TIME_LIMIT_HOURS)).isoformat()
    query = f"delete from binance_trades_24h  where ts < '{cutoff}'"
    print(f"[CLEANUP] Removing rows older than {cutoff}")
    try:
        sb.rpc("exec_sql_v2", {"query": query}).execute()
    except Exception as e:
        print(f"[WARN] Cleanup skipped: {e}")

def ingest_trades():
    symbols = get_all_usdt_symbols()
    print(f"[INFO] Found {len(symbols)} USDT pairs")

    for symbol in symbols:
        try:
            trades = fetch_trades(symbol)
            rows = []
            for t in trades:
                ts = datetime.fromtimestamp(t["time"] / 1000.0, tz=timezone.utc)
                if ts < datetime.now(timezone.utc) - timedelta(hours=TIME_LIMIT_HOURS):
                    continue  # skip anything older than 24h

                rows.append({
                    "symbol": symbol,
                    "trade_id": t["id"],
                    "price": float(t["price"]),
                    "qty": float(t["qty"]),
                    "quote_qty": float(t["quoteQty"]),
                    "side": "BUY" if not t["isBuyerMaker"] else "SELL",
                    "is_buyer_maker": t["isBuyerMaker"],
                    "ts": ts.isoformat()
                })

            if rows:
                sb.table("binance_trades_24h").upsert(rows, on_conflict=["symbol", "trade_id"]).execute()
                print(f"[{symbol}] Inserted {len(rows)} trades")

        except Exception as e:
            print(f"[ERROR] {symbol}: {e}")
            time.sleep(0.3)

    # cleanup old data after each full pass
    cleanup_old_rows()

if __name__ == "__main__":
    while True:
        try:
            ingest_trades()
        except Exception as e:
            print("[FATAL ERROR]", e)
        time.sleep(300)  # run every 5 minutes


