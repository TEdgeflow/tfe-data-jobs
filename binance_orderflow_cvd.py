import os, time, requests
from datetime import datetime, timezone
from supabase import create_client, Client

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
LIMIT_SYMBOLS = int(os.getenv("LIMIT_SYMBOLS", "50"))

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BINANCE_FAPI = "https://fapi.binance.com/fapi/v1"

# ========= FETCH SYMBOLS =========
def get_perp_symbols():
    resp = requests.get(f"{BINANCE_FAPI}/exchangeInfo")
    resp.raise_for_status()
    symbols = [
        s["symbol"] for s in resp.json()["symbols"]
        if s["contractType"] == "PERPETUAL" and s["quoteAsset"] == "USDT"
    ]
    return symbols[:LIMIT_SYMBOLS]

# ========= FETCH ORDERFLOW (buys, sells, VWAP) =========
def fetch_orderflow(symbol):
    trades = requests.get(f"{BINANCE_FAPI}/aggTrades", params={"symbol": symbol, "limit": 1000})
    trades.raise_for_status()
    data = trades.json()

    buys, sells, vwap_num, vwap_den = 0, 0, 0, 0
    for t in data:
        qty = float(t["q"])
        price = float(t["p"])
        if t["m"]:  # buyer is market maker → sell
            sells += qty
        else:       # aggressive buy
            buys += qty
        vwap_num += price * qty
        vwap_den += qty

    delta = buys - sells
    vwap = vwap_num / vwap_den if vwap_den > 0 else None
    return buys, sells, delta, vwap

# ========= FETCH FUNDING + OI =========
def fetch_funding_and_oi(symbol):
    try:
        funding_resp = requests.get(f"{BINANCE_FAPI}/premiumIndex", params={"symbol": symbol})
        funding_resp.raise_for_status()
        funding_rate = float(funding_resp.json().get("lastFundingRate", 0))

        oi_resp = requests.get(f"{BINANCE_FAPI}/openInterest", params={"symbol": symbol})
        oi_resp.raise_for_status()
        open_interest = float(oi_resp.json().get("openInterest", 0))

        return funding_rate, open_interest
    except Exception as e:
        print(f"[warn] Funding/OI error for {symbol}:", e)
        return 0.0, 0.0

# ========= UPSERT =========
cvd_tracker = {}

def upsert_orderflow(symbol, buys, sells, delta, vwap, funding_rate, open_interest):
    prev_cvd = cvd_tracker.get(symbol, 0)
    cvd = prev_cvd + delta
    cvd_tracker[symbol] = cvd

    row = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "symbol": symbol,
        "buys_volume": buys,
        "sells_volume": sells,
        "delta": delta,
        "cvd": cvd,
        "funding_rate": funding_rate,
        "open_interest": open_interest,
        "vwap": vwap
    }

    sb.table("orderflow_cvd").upsert(row).execute()
    print(f"[upsert] {symbol} delta={delta:.2f} cvd={cvd:.2f} vwap={vwap:.2f if vwap else 0}")

# ========= MAIN LOOP =========
def main():
    symbols = get_perp_symbols()
    print(f"[symbols] Found {len(symbols)} USDT-PERP symbols.")

    while True:
        for sym in symbols:
            try:
                buys, sells, delta, vwap = fetch_orderflow(sym)
                funding_rate, open_interest = fetch_funding_and_oi(sym)
                upsert_orderflow(sym, buys, sells, delta, vwap, funding_rate, open_interest)
            except Exception as e:
                print(f"[error] {sym}:", e)

        time.sleep(60)  # update every 1 minute

if __name__ == "__main__":
    main()



