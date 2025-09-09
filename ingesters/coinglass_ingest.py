import os
import requests
from datetime import datetime, timezone
from supabase import create_client, Client

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
COINGLASS_API_KEY = os.getenv("COINGLASS_API_KEY")

if not SUPABASE_URL or not SUPABASE_KEY or not COINGLASS_API_KEY:
    raise RuntimeError("Missing environment variables")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE_URL = "https://open-api.coinglass.com/api/pro"

HEADERS = {
    "accept": "application/json",
    "coinglassSecret": COINGLASS_API_KEY
}


def fetch_data(endpoint, params=None):
    url = f"{BASE_URL}/{endpoint}"
    r = requests.get(url, headers=HEADERS, params=params or {})
    r.raise_for_status()
    return r.json()


def ingest_master():
    # containers per symbol
    records = {}

    # --- 1. Funding Rates ---
    funding = fetch_data("futures/funding")
    for f in funding.get("data", []):
        sym = f["symbol"]
        ts = datetime.now(timezone.utc)  # coinglass doesn’t always give exact ts
        key = (sym, ts)
        if key not in records:
            records[key] = {}
        records[key]["funding_rate"] = f.get("fundingRate")

    # --- 2. Open Interest ---
    oi = fetch_data("futures/openInterest")
    for o in oi.get("data", []):
        sym = o["symbol"]
        ts = datetime.now(timezone.utc)
        key = (sym, ts)
        if key not in records:
            records[key] = {}
        records[key]["open_interest"] = o.get("openInterest")

    # --- 3. Liquidations ---
    liq = fetch_data("futures/liquidation")
    for l in liq.get("data", []):
        sym = l["symbol"]
        ts = datetime.now(timezone.utc)
        key = (sym, ts)
        if key not in records:
            records[key] = {}
        records[key]["liq_side"] = l.get("side")
        records[key]["liq_amount"] = l.get("amount")
        records[key]["liq_price"] = l.get("price")
        records[key]["liq_interval"] = l.get("interval")

    # --- 4. Liquidity Levels (orderbook depth) ---
    depth = fetch_data("futures/liquidity")
    for d in depth.get("data", []):
        sym = d["symbol"]
        ts = datetime.now(timezone.utc)
        key = (sym, ts)
        if key not in records:
            records[key] = {}
        records[key]["bid_liquidity"] = d.get("bidLiquidity")
        records[key]["ask_liquidity"] = d.get("askLiquidity")

    # --- Insert into Supabase ---
    rows = []
    for (sym, ts), vals in records.items():
        row = {
            "symbol": sym,
            "ts": ts.isoformat(),
            "funding_rate": vals.get("funding_rate"),
            "open_interest": vals.get("open_interest"),
            "liq_side": vals.get("liq_side"),
            "liq_amount": vals.get("liq_amount"),
            "liq_price": vals.get("liq_price"),
            "liq_interval": vals.get("liq_interval"),
            "bid_liquidity": vals.get("bid_liquidity"),
            "ask_liquidity": vals.get("ask_liquidity"),
        }
        rows.append(row)

    if rows:
        sb.table("derivatives_master").insert(rows).execute()
        print(f"Inserted {len(rows)} rows into derivatives_master")
    else:
        print("No rows to insert")


if __name__ == "__main__":
    ingest_master()















