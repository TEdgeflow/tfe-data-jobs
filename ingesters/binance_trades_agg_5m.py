import os
import requests
import time
from datetime import datetime, timezone
from supabase import create_client, Client

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
BINANCE_URL = "https://api.binance.com/api/v3/trades"

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing Supabase credentials")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ========= HELPERS =========
def get_binance_trades(symbol="BTCUSDT", limit=1000):
    """Fetch latest trades from Binance"""
    url = f"{BINANCE_URL}?symbol={symbol}&limit={limit}"
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.json()

def bucketize(ts: int):
    """Return 5m bucket from trade timestamp"""
    dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    bucket_5m = dt.replace(minute=(dt.minute // 5) * 5, second=0, microsecond=0)
    return bucket_5m

def process_trades(symbol="BTCUSDT"):
    """Fetch trades, bucketize, and upsert into binance_trades_agg_5m"""
    trades = get_binance_trades(symbol=symbol)
    agg_map = {}

    for t in trades:
        ts = t["time"]
        qty = float(t["qty"])
        price = float(t["price"])
        quote_qty = qty * price
        is_buyer_maker = t["isBuyerMaker"]

        side = "SELL" if is_buyer_maker else "BUY"
        bucket_5m = bucketize(ts)

        key = bucket_5m

        if key not in agg_map:
            agg_map[key] = {
                "symbol": symbol,
                "bucket_5m": bucket_5m.isoformat(),
                "buy_vol": 0,
                "sell_vol": 0,
                "delta": 0,
                "bullish_trades": 0,
                "bearish_trades": 0,
            }

        row = agg_map[key]
        if side == "BUY":
            row["buy_vol"] += quote_qty
            row["delta"] += quote_qty
            row["bullish_trades"] += 1
        else:
            row["sell_vol"] += quote_qty
            row["delta"] -= quote_qty
            row["bearish_trades"] += 1

    # CVD = delta (per 5m window here)
    for row in agg_map.values():
        row["cvd"] = row["delta"]

# Ensure all datetime fields are strings
for row in agg_map.values():
    if isinstance(row["bucket_5m"], datetime):
        row["bucket_5m"] = row["bucket_5m"].isoformat()

rows = list(agg_map.values())
if rows:
    try:
        sb.table("binance_trades_agg_5m").upsert(rows).execute()
        print(f"✅ {symbol} → {len(rows)} rows")
    except Exception as e:
        print(f"[DB error] {symbol}: {e}")


# ========= MAIN LOOP =========
def main():
    symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]  # extend later with all USDT pairs
    while True:
        for sym in symbols:
            try:
                process_trades(symbol=sym)
            except Exception as e:
                print(f"[error] {sym}: {e}")
        time.sleep(300)  # run every 5 minutes

if __name__ == "__main__":
    main()


