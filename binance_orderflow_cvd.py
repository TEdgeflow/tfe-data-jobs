import os
import time
import requests
from datetime import datetime, timezone
from supabase import create_client, Client

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
LIMIT_SYMBOLS = int(os.getenv("LIMIT_SYMBOLS", "200"))

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BINANCE_FAPI = "https://fapi.binance.com"

# ========= HELPERS =========
def iso_now():
    return datetime.now(timezone.utc).isoformat()

def get_symbols():
    """Fetch all USDT perpetual pairs"""
    resp = requests.get(f"{BINANCE_FAPI}/fapi/v1/exchangeInfo")
    resp.raise_for_status()
    symbols = [
        s["symbol"] for s in resp.json()["symbols"]
        if s["contractType"] == "PERPETUAL" and s["quoteAsset"] == "USDT"
    ]
    return symbols[:LIMIT_SYMBOLS]

def fetch_orderflow(symbol):
    """Get buy/sell volume and delta from trades"""
    url = f"{BINANCE_FAPI}/fapi/v1/trades?symbol={symbol}&limit=1000"
    resp = requests.get(url)
    resp.raise_for_status()
    trades = resp.json()

    buy_vol = sum(float(t["qty"]) for t in trades if not t["isBuyerMaker"])
    sell_vol = sum(float(t["qty"]) for t in trades if t["isBuyerMaker"])
    delta = buy_vol - sell_vol
    return buy_vol, sell_vol, delta

def fetch_funding_oi(symbol):
    """Get funding rate + OI for symbol"""
    f_resp = requests.get(f"{BINANCE_FAPI}/fapi/v1/fundingRate", params={"symbol": symbol, "limit": 1})
    f_resp.raise_for_status()
    funding_rate = float(f_resp.json()[0]["fundingRate"]) if f_resp.json() else None

    oi_resp = requests.get(f"{BINANCE_FAPI}/fapi/v1/openInterest", params={"symbol": symbol})
    oi_resp.raise_for_status()
    open_interest = float(oi_resp.json()["openInterest"]) if oi_resp.json() else None

    return funding_rate, open_interest

def fetch_vwap(symbol):
    """Get VWAP from recent 1m candles"""
    url = f"{BINANCE_FAPI}/fapi/v1/klines?symbol={symbol}&interval=1m&limit=50"
    resp = requests.get(url)
    resp.raise_for_status()
    klines = resp.json()

    num, den = 0.0, 0.0
    for k in klines:
        high, low, close, vol = float(k[2]), float(k[3]), float(k[4]), float(k[5])
        price = (high + low + close) / 3
        num += price * vol
        den += vol
    return num / den if den else None

def upsert(symbol, buy_vol, sell_vol, delta, funding_rate, open_interest, vwap):
    row = {
        "ts": iso_now(),
        "symbol": symbol,
        "buys_volume": buy_vol,
        "sells_volume": sell_vol,
        "delta": delta,
        "funding_rate": funding_rate,
        "open_interest": open_interest,
        "vwap": vwap,
    }
    try:
        res = sb.table("orderflow_cvd").upsert(row).execute()
        print(f"[upsert] {symbol} Δ={delta:.2f} FR={funding_rate} OI={open_interest} VWAP={vwap} → {res}")
    except Exception as e:
        print(f"[upsert-error] {symbol}: {e}")

# ========= MAIN LOOP =========
def main():
    while True:
        try:
            symbols = get_symbols()
            print(f"[symbols] Found {len(symbols)} USDT-PERP symbols.")

            for sym in symbols:
                try:
                    buy_vol, sell_vol, delta = fetch_orderflow(sym)
                    funding_rate, open_interest = fetch_funding_oi(sym)
                    vwap = fetch_vwap(sym)

                    upsert(sym, buy_vol, sell_vol, delta, funding_rate, open_interest, vwap)
                except Exception as e:
                    print(f"[error] {sym}:", e)

            print("✅ Cycle complete.")
        except Exception as e:
            print("Fatal error:", e)

        time.sleep(300)  # every 5 min

if __name__ == "__main__":
    main()




