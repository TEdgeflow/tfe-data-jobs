import os
import time
from datetime import datetime, timedelta, timezone
import requests
from supabase import create_client, Client

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
BINANCE_URL = "https://api.binance.com/api/v3/aggTrades"

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_trades(symbol: str, start_time: int, end_time: int):
    """Fetch trades from Binance aggTrades endpoint"""
    params = {
        "symbol": symbol,
        "startTime": start_time,
        "endTime": end_time,
        "limit": 1000
    }
    resp = requests.get(BINANCE_URL, params=params)
    resp.raise_for_status()
    return resp.json()

def bucket_5m(ts: int) -> datetime:
    dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    floored = dt - timedelta(minutes=dt.minute % 5,
                             seconds=dt.second,
                             microseconds=dt.microsecond)
    return floored

def ingest_symbol(symbol: str):
    now = datetime.now(timezone.utc)
    start = now - timedelta(minutes=10)  # look back slightly for safety
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(now.timestamp() * 1000)

    trades = fetch_trades(symbol, start_ms, end_ms)
    if not trades:
        return

    rows = {}
    for t in trades:
        price = float(t["p"])
        qty = float(t["q"])
        is_buyer_maker = t["m"]
        ts = int(t["T"])
        bucket = bucket_5m(ts)

        key = (symbol, bucket)
        if key not in rows:
            rows[key] = {
                "symbol": symbol,
                "bucket_5m": bucket.isoformat(),
                "buy_vol": 0,
                "sell_vol": 0,
                "delta": 0,
                "cvd": 0,
                "bullish_trades": 0,
                "bearish_trades": 0,
            }

        if is_buyer_maker:
            rows[key]["sell_vol"] += qty
            rows[key]["delta"] -= qty
            rows[key]["bearish_trades"] += 1
        else:
            rows[key]["buy_vol"] += qty
            rows[key]["delta"] += qty
            rows[key]["bullish_trades"] += 1

        # running CVD = delta (rolling sum is handled in downstream view if needed)
        rows[key]["cvd"] += rows[key]["delta"]

    # Upsert to Supabase
    data = list(rows.values())
    sb.table("binance_trades_agg_5m").upsert(data).execute()
    print(f"Upserted {len(data)} rows for {symbol}")

def run_all():
    symbols = os.getenv("BINANCE_SYMBOLS", "BTCUSDT,ETHUSDT").split(",")
    for s in symbols:
        ingest_symbol(s.strip())

if __name__ == "__main__":
    while True:
        run_all()
        time.sleep(300)  # every 5 minutes
