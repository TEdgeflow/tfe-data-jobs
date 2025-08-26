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
        "Authorization": f"Bearer {NANSEN_API_KEY}",
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

    if resp.status_code != 200:
        raise Exception(f"Nansen API error {resp.status_code}: {resp.text}")

    data = resp.json()
    rows = []

    for item in data.get("data", []):
        rows.append({
            "ts": datetime.now(timezone.utc).isoformat(),
            "token": item.get("symbol"),
            "chain": item.get("chain"),
            "inflow_usd": item.get("inflowUsd", 0),
            "outflow_usd": item.get("outflowUsd", 0),
            "netflow_usd": item.get("netflowUsd", 0),
            "sm_category": item.get("smCategory")
        })

    return rows

def upsert_whale_flows(rows):
    if rows:
        sb.table("nansen_whaleflows").upsert(rows).execute()
        print(f"[upsert] {len(rows)} whale flow rows inserted.")
    else:
        print("[skip] No rows to insert.")

def main():
    while True:
        try:
            print("Fetching whale flows from Nansen…")
            rows = fetch_whale_flows()
            upsert_whale_flows(rows)
            print("Done.")
        except Exception as e:
            print("Error during Nansen whale flow job:", e)

        time.sleep(600)  # every 10 minutes

if __name__ == "__main__":
    main()
