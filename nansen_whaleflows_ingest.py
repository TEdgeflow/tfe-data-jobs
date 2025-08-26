import os
import time
import requests
from datetime import datetime, timezone
from supabase import create_client, Client

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
NANSEN_API_KEY = os.getenv("NANSEN_API_KEY")

if not SUPABASE_URL or not SUPABASE_KEY or not NANSEN_API_KEY:
    raise RuntimeError("Missing one of SUPABASE_URL, SUPABASE_KEY, or NANSEN_API_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ========= Nansen Endpoint =========
NANSEN_URL = "https://api.nansen.ai/api/beta/smart-money/inflows"

def fetch_whale_flows():
    headers = {
        "x-api-key": NANSEN_API_KEY,   # ✅ FIXED header
        "Content-Type": "application/json"
    }
    body = {
        "parameters": {
            "smFilter": ["180D Smart Trader", "Fund", "Smart Trader"],
            "chains": ["ethereum", "solana"],
            "includeStablecoin": True,
            "includeNativeTokens": True
        }
    }
    resp = requests.post(NANSEN_URL, headers=headers, json=body)
    resp.raise_for_status()
    return resp.json()

def upsert_whale_flows(data):
    rows = []
    for d in data.get("data", []):
        rows.append({
            "ts": datetime.now(timezone.utc).isoformat(),
            "token": d.get("tokenSymbol"),
            "chain": d.get("chain"),
            "inflow_usd": d.get("inflowUsd"),
            "outflow_usd": d.get("outflowUsd"),
            "netflow_usd": d.get("netflowUsd"),
            "sm_category": d.get("smCategory")
        })
    if rows:
        sb.table("nansen_whaleflows").upsert(rows).execute()
        print(f"[upsert] {len(rows)} whale flow rows")

def main():
    while True:
        try:
            print("[debug] SUPABASE_URL present?", bool(SUPABASE_URL))
            print("[debug] SUPABASE_KEY present?", bool(SUPABASE_KEY))
            print("[debug] NANSEN_API_KEY present?", bool(NANSEN_API_KEY))

            data = fetch_whale_flows()
            upsert_whale_flows(data)
            print("Done whale flows.")
        except Exception as e:
            print("Error whale flows job:", e)
        time.sleep(3600)  # run every 1 hour

if __name__ == "__main__":
    main()
