import os, time, requests
from supabase import create_client, Client
from datetime import datetime, timezone

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BINANCE_FAPI = "https://fapi.binance.com"

# ========= Helpers =========
def iso_now():
    return datetime.now(timezone.utc).isoformat()

def get_usdt_perp_symbols():
    """Fetch all USDT perpetual contract symbols"""
    resp = requests.get(f"{BINANCE_FAPI}/fapi/v1/exchangeInfo")
    resp.raise_for_status()
    symbols = [
        s["symbol"] for s in resp.json()["symbols"]
        if s["quoteAsset"] == "USDT" and s["contractType"] == "PERPETUAL"
    ]
    return symbols

def fetch_trades(symbol, limit=500):
    """Fetch recent trades for a symbol"""
    url = f"{BINANCE_FAPI}/fapi/v1/trades"
    resp = requests.get(url, params={"symbol": symbol, "limit": limit})
    resp.raise_for_status()
    return resp.json()

def process_trades(symbol, trades):
    """Calculate buy/sell volume and delta"""
    buys, sells = 0, 0
    for t in trades:
        qty = float(t["qty"])
        if t["isBuyerMaker"]:  # seller-initiated
            sells += qty
        else:  # buyer-initiated
            buys += qty
    delta = buys - sells
    return {
        "ts": iso_now(),
        "symbol": symbol,
        "buys_volume": buys,
        "sells_volume": sells,
        "delta": delta,
        "cvd": delta  # running sum handled later if needed
    }

def upsert(rows):
    if rows:
        sb.table("orderflow_cvd").upsert(rows).execute()
        print(f"[upsert] {len(rows)} rows")

# ========= Main Job =========
def main():
    symbols = get_usdt_perp_symbols()
    print(f"[symbols] Found {len(symbols)} USDT-PERP symbols.")

    batch_size = 100
    while True:
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i+batch_size]
            rows = []
            for sym in batch:
                try:
                    trades = fetch_trades(sym, limit=500)
                    row = process_trades(sym, trades)
                    rows.append(row)
                except Exception as e:
                    print(f"[error] {sym}: {e}")
            upsert(rows)
            time.sleep(5)  # wait between batches to stay under rate limits

        print("Cycle complete. Restarting...")
        time.sleep(60)  # wait 1 min before full refresh

if __name__ == "__main__":
    main()

