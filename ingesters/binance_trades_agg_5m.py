import os
import time
import requests
from datetime import datetime, timedelta, timezone
from supabase import create_client, Client

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
LIMIT_SYMBOLS = int(os.getenv("LIMIT_SYMBOLS", "0"))

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing required environment variables")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ========= HELPERS =========
def get_all_symbols():
    """Fetch all Binance USDT pairs"""
    url = "https://api.binance.com/api/v3/exchangeInfo"
    resp = requests.get(url).json()
    symbols = [s["symbol"] for s in resp["symbols"] if s["quoteAsset"] == "USDT"]
    if LIMIT_SYMBOLS > 0:
        symbols = symbols[:LIMIT_SYMBOLS]
    return symbols

def fetch_trades(symbol):
    """Fetch last 5 minutes of trades for a symbol"""
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=5)
    url = "https://api.binance.com/api/v3/aggTrades"
    params = {
        "symbol": symbol,
        "startTime": int(start.timestamp() * 1000),
        "endTime": int(end.timestamp() * 1000)
    }
    r = requests.get(url, params=params)
    r.raise_for_status()
    return r.json()

def ingest_symbol(symbol):
    """Aggregate 5m trades into Supabase"""
    trades = fetch_trades(symbol)
    if not trades:
        return

    buy_vol, sell_vol = 0, 0
    bullish_trades, bearish_trades = 0, 0
    delta, cvd = 0, 0

    for t in trades:
        qty = float(t["q"])
        price = float(t["p"])
        quote_qty = qty * price
        is_buyer_maker = t["m"]

        if is_buyer_maker:  # sell
            sell_vol += quote_qty
            delta -= quote_qty
            bearish_trades += 1
        else:  # buy
            buy_vol += quote_qty
            delta += quote_qty
            bullish_trades += 1

        cvd += delta

    bucket_5m = datetime.now(timezone.utc).replace(second=0, microsecond=0)

    row = {
        "symbol": symbol,
        "bucket_5m": bucket_5m,
        "buy_vol": buy_vol,
        "sell_vol": sell_vol,
        "delta": delta,
        "cvd": cvd,
        "bullish_trades": bullish_trades,
        "bearish_trades": bearish_trades,
    }

    sb.table("binance_trades_agg_5m").upsert(row).execute()
    print(f"Upserted 1 row for {symbol}")

def run_all():
    symbols = get_all_symbols()
    for s in symbols:
        try:
            ingest_symbol(s.strip())
        except Exception as e:
            print(f"Error for {s}: {e}")

if __name__ == "__main__":
    while True:
        run_all()
        time.sleep(300)  # every 5 minutes

