import os
import time
import requests
from supabase import create_client, Client
from datetime import datetime, timezone

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
NANSEN_API_KEY = os.getenv("NANSEN_API_KEY")

if not SUPABASE_URL or not SUPABASE_KEY or not NANSEN_API_KEY:
    raise RuntimeError("Missing SUPABASE_URL, SUPABASE_KEY, or NANSEN_API_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ========= Nansen Endpoint =========
# ✅ official docs: https://api.nansen.ai/smart-money/holdings
NANSEN_URL = "https://api.nansen.ai/smart-money/holdings"

def fetch_holdings():
    headers = {
        "Authorization": f"Bearer {NANSEN_API_KEY}",  # ✅ use Bearer auth
        "Content-Type": "application/json"
    }
    body = {
        "parameters": {
            "chains": ["ethereum", "solana"],
            "smFilter": ["180D Smart Trader", "Fund", "Smart Trader"],
            "includeStablecoin": True,
            "includeNativeTokens": True,
            "excludeSmFilter": []
        },
        "pagination": {
            "page": 1,
            "recordsPerPage": 100
        }
    }

    resp = requests.post(NANSEN_URL, headers=headers, json=body)

    # Debugging info
    print("[debug] status:", resp.status_code)
    print("[debug] response (first 500 chars):", resp.text[:500])

    if resp.status_code != 200:
        # Raise error but also give us full info
        raise RuntimeError(f"Nansen API error {resp.status_code}: {resp.text}")

    return resp.json()

def upsert_holdings(data):
    rows = []
    # Holdings returns a list of token objects
    for d in data:
        rows.append({
            "ts": datetime.now(timezone.utc).isoformat(),
            "token": d.get("symbol"),
            "chain": d.get("chain"),
            "holding_value": d.get("holdingUsd"),
            "holding_change": d.get("changeUsd"),
            "sm_category": ",".join(d.get("sectors", [])) if d.get("sectors") else None
        })
    if rows:
        sb.table("nansen_holdings").upsert(rows).execute()
        print(f"[upsert] {len(rows)} holdings rows")

def main():
    while True:
        try:
            data = fetch_holdings()
            if isinstance(data, dict) and "data" in data:
                # Some Nansen APIs wrap results in {"data": [...]}
                upsert_holdings(data["data"])
            elif isinstance(data, list):
                upsert_holdings(data)
            else:
                print("⚠️ Unexpected response format:", type(data), data)
            print("✅ Done holdings.")
        except Exception as e:
            print("❌ Error holdings job:", e)
        time.sleep(3600)  # Run every 1 hour

if __name__ == "__main__":
    main()
