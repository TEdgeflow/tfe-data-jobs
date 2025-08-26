import os
import time
import requests
from datetime import datetime, timezone
from supabase import create_client, Client

# ======== ENV VARS ========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BINANCE_API = "https://fapi.binance.com"

# keep CVD state per symbol
cvd_state = {}

def iso_now():
    return datetime.now(timezone.utc).isoformat()

def fetch_trades(symbol, limit=1000):
    """Fetch recent trades from Binance futures"""
    url = f"{BINANCE_API}/fapi/v1/trades"
    resp = requests.get(url, params={"symbol": symbol, "limit": limit})
    resp.raise_for_status()
    return resp.json()

def fetch_funding(symbol):
    """Fetch latest funding rate"""
    url = f"{BINANCE_API}/fapi/v1/fundingRate"
    resp = requests.get(url, params={"symbol": symbol, "limit": 1})
    resp.raise_for_status()
    data = resp.json()
    return float(data[0]["fundingRate"]) if data else None

def fetch_oi(symbol):
    """Fetch latest open interest"""
    url = f"{BINANCE_API}/fapi/v1/openInterest"
    resp = requests.get(url, params={"symbol": symbol})
    resp.raise_for_status()
    data = resp.json()
    return float(data["openInterest"]) if "openInterest" in data else None

def process_trades(symbol, trades):
    """Calculate delta and update cumulative delta"""
    buy_vol, sell_vol = 0, 0
    for t in trades:
        qty = float(t["qty"])
        if t["isBuyerMaker"]:  # SELL order
            sell_vol += qty
        else:  # BUY order
            buy_vol += qty

    delta = buy_vol - sell_vol
    prev_cvd = cvd_state.get(symbol, 0)
    cvd = prev_cvd + delta
    cvd_state[symbol] = cvd
    return buy_vol, sell_vol, delta, cvd

def upsert(symbol, buy_vol, sell_vol, delta, cvd, funding, oi):
    row = {
        "ts": iso_now(),
        "symbol": symbol,
        "buys_volume": buy_vol,
        "sells_volume": sell_vol,
        "delta": delta,
        "cvd": cvd,
        "funding_rate": funding,
        "open_interest": oi
    }
    sb.table("orderflow_cvd").upsert(row).execute()
    print(f"[upsert] {symbol} Δ={delta:.2f} CVD={cvd:.2f} FR={funding} OI={oi}")

def main():
    symbols = [s["symbol"] for s in requests.get(
        f"{BINANCE_API}/fapi/v1/exchangeInfo").json()["symbols"]
        if s["contractType"] == "PERPETUAL" and s["quoteAsset"] == "USDT"
    ]

    print(f"[symbols] Tracking {len(symbols)} perp pairs...")

    while True:
        try:
            for sym in symbols:
                try:
                    trades = fetch_trades(sym, limit=1000)
                    buy_vol, sell_vol, delta, cvd = process_trades(sym, trades)
                    funding = fetch_funding(sym)
                    oi = fetch_oi(sym)
                    upsert(sym, buy_vol, sell_vol, delta, cvd, funding, oi)
                except Exception as e:
                    print(f"[error] {sym}: {e}")

            time.sleep(60)  # every 1 min
        except Exception as e:
            print("Fatal loop error:", e)
            time.sleep(10)

if __name__ == "__main__":
    main()



