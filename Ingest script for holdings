import os, time, requests
from supabase import create_client, Client
from datetime import datetime, timezone

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
NANSEN_API_KEY = os.getenv("NANSEN_API_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

NANSEN_BASE = "https://api.nansen.ai/api/beta"

def fetch_holdings():
    url = f"{NANSEN_BASE}/smart-money/holdings"
    headers = {
        "Authorization": f"Bearer {NANSEN_API_KEY}",
        "Content-Type": "application/json"
    }
    body = {
        "parameters": {
            "smFilter": ["180D Smart Trader", "Fund", "Smart Trader"],
            "chains": ["ethereum", "solana"],
            "includeStablecoin": True
        }
    }
    resp = requests.post(url, headers=headers, json=body)
    resp.raise_for_status()
    return resp.json()

def upsert_holdings(data):
    rows = []
    for d in data.get("data", []):
        rows.append({
            "ts": datetime.now(timezone.utc).isoformat(),
            "token": d.get("tokenSymbol"),
            "chain": d.get("chain"),
            "holding_value": d.get("holdingUsd", 0),
            "holding_change": d.get("changeUsd", 0)
        })
    if rows:
        sb.table("nansen_holdings").upsert(rows).execute()
        print(f"[upsert] {len(rows)} holdings rows")
    else:
        print("[skip] No holdings rows")

def main():
    while True:
        try:
            print("Fetching Nansen holdings…")
            data = fetch_holdings()
            upsert_holdings(data)
            print("✅ Done holdings.")
        except Exception as e:
            print("Error holdings job:", e)
        time.sleep(3600)  # run every 1 hour

if __name__ == "__main__":
    main()
