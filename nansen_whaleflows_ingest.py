import os
import time
import requests
from datetime import datetime, timezone
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
NANSEN_API_KEY = os.getenv("NANSEN_API_KEY")

if not SUPABASE_URL or not SUPABASE_KEY or not NANSEN_API_KEY:
    raise RuntimeError("Missing one of SUPABASE_URL, SUPABASE_KEY, or NANSEN_API_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

NANSEN_URL = "https://api.nansen.ai/api/beta/smart-money/inflows"

def fetch_whale_flows():
    headers = {"apiKey": NANSEN_API_KEY, "Content-Type": "application/json"}
    body = {
        "parameters": {
            "smFilter": ["180D Smart Trader", "Fund", "Smart Trader"],
            "chains": ["ethereum", "solana"],
            "includeStablecoin": True,
            "includeNativeTokens": True
        }
    }
    resp = requests.post(NANSEN_URL, headers=headers, json=body)
    print("[debug] status", resp.status_code, resp.text[:300])
    resp.raise_for_status()
    return resp.json()

def upsert_whale_flows(data):
    rows = []
    # Nansen returns a list directly, not { "data": [...] }
    for d in data:  
        rows.append({
            "ts": datetime.now(timezone.utc).isoformat(),
            "token": d.get("symbol"),
            "chain": d.get("chain"),
            "inflow_usd": d.get("volume24hUSD"),   # using 24h as inflow proxy
            "outflow_usd": None,                   # Nansen doesn’t provide directly
            "netflow_usd": None,                   # could be computed if available
            "sm_category": ",".join(d.get("sectors", [])) if d.get("sectors") else None
        })
    if rows:
        sb.table("nansen_whaleflows").upsert(rows).execute()
        print(f"[upsert] {len(rows)} whale flow rows")

def main():
    while True:
        try:
            data = fetch_whale_flows()
            upsert_whale_flows(data)
            print("Done whale flows.")
        except Exception as e:
            print("Error whale flows job:", e)
        time.sleep(3600)

if __name__ == "__main__":
    main()



