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
    """Return 15m, 1h, 1d buckets from trade timestamp"""
    dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    bucket_15m = dt.replace(minute=(dt.minute // 15) * 15, second=0, microsecond=0)
    bucket_1h = dt.replace(minute=0, second=0, microsecond=0)
    bucket_1d = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return bucket_15m, bucket_1h, bucket_1d

def process_trades(symbol="BTCUSDT"):
    """Fetch trades, bucketize, and upsert into binance_trades_agg"""
    trades = get_binance_trades(symbol=symbol)
    agg_map = {}

    for t in trades:
        ts = t["time"]
        qty = float(t["qty"])
        price = float(t["price"])
        quote_qty = qty * price
        is_buyer_maker = t["isBuyerMaker"]

        side = "SELL" if is_buyer_maker else "BUY"
        bucket_15m, bucket_1h, bucket_1d = bucketize(ts)

        key = (bucket_15m, bucket_1h, bucket_1d)

        if key not in agg_map:
            agg_map[key] = {
                "symbol": symbol,
                "bucket_15m": bucket_15m.isoformat(),
                "bucket_1h": bucket_1h.isoformat(),
                "bucket_1d": bucket_1d.isoformat(),
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

    # Calculate CVD (cumulative delta over time)
    # ✅ Here we do it simply per run, you can later extend with windowed CV
    for row in agg_map.values():
        row["cvd"] = row["delta"]

    rows = list(agg_map.values())
    if rows:
        sb.table("binance_trades_agg").upsert(rows).execute()
        print(f"✅ Upserted {len(rows)} rows into binance_trades_agg")

# ========= MAIN LOOP =========
def main():
    symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]  # extend later
    while True:
        for sym in symbols:
            try:
                process_trades(symbol=sym)
            except Exception as e:
                print(f"[error] {sym}: {e}")
        time.sleep(60)  # fetch every 1 min

if __name__ == "__main__":
    main()

