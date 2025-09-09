import os
import requests
from datetime import datetime, timezone
from supabase import create_client, Client

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
COINGLASS_API_KEY = os.getenv("COINGLASS_API_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE_URL = "https://open-api.coinglass.com/api/pro"
HEADERS = {"accept": "application/json", "coinglassSecret": COINGLASS_API_KEY}

def fetch(endpoint, params=None):
    url = f"{BASE_URL}/{endpoint}"
    r = requests.get(url, headers=HEADERS, params=params or {})
    r.raise_for_status()
    return r.json()

def ingest_all():
    now = datetime.now(timezone.utc)

    # 1. Get all supported symbols
    supported = fetch("futures/symbols")
    symbols = [s["symbol"] for s in supported.get("data", [])]

    funding_rows = []
    oi_rows = []
    liq_rows = []
    lq_rows = []

    for sym in symbols:
        try:
            # --- Funding Rate (global avg) ---
            fr_data = fetch("futures/funding", {"symbol": sym})
            if fr_data.get("data"):
                funding_rate = fr_data["data"][0].get("avgFundingRate")
                funding_rows.append({
                    "symbol": sym,
                    "funding_rate": funding_rate,
                    "timestamp": now.isoformat()
                })

            # --- Open Interest (global) ---
            oi_data = fetch("futures/openInterest", {"symbol": sym})
            if oi_data.get("data"):
                open_interest = oi_data["data"][0].get("openInterest")
                oi_rows.append({
                    "symbol": sym,
                    "oi": open_interest,
                    "timestamp": now.isoformat()
                })

            # --- Liquidations ---
            liq_data = fetch("futures/liquidation", {"symbol": sym})
            for l in liq_data.get("data", []):
                liq_rows.append({
                    "symbol": sym,
                    "side": l.get("side"),
                    "amount": l.get("amount"),
                    "price": l.get("price"),
                    "time_interval": l.get("interval"),
                    "ts": now.isoformat()
                })

            # --- Liquidity Levels ---
            lq_data = fetch("futures/liquidity", {"symbol": sym})
            if lq_data.get("data"):
                lq_rows.append({
                    "symbol": sym,
                    "bid_liquidity": lq_data["data"][0].get("bidLiquidity"),
                    "ask_liquidity": lq_data["data"][0].get("askLiquidity"),
                    "ts": now.isoformat()
                })

        except Exception as e:
            print(f"[WARN] Failed for {sym}: {e}")

    # === Insert into Supabase ===
    if funding_rows:
        sb.table("derivatives_funding").insert(funding_rows).execute()
        print(f"Inserted {len(funding_rows)} funding rows")

    if oi_rows:
        sb.table("derivatives_oi").insert(oi_rows).execute()
        print(f"Inserted {len(oi_rows)} OI rows")

    if liq_rows:
        sb.table("derivatives_liquidations").insert(liq_rows).execute()
        print(f"Inserted {len(liq_rows)} liquidation rows")

    if lq_rows:
        sb.table("derivatives_liquidity_levels").insert(lq_rows).execute()
        print(f"Inserted {len(lq_rows)} liquidity rows")

if __name__ == "__main__":
    ingest_all()




