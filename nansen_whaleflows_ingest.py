import os, time, requests
from datetime import datetime, timezone
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
NANSEN_API_KEY = os.getenv("NANSEN_API_KEY")

print("[debug] SUPABASE_URL present?", bool(SUPABASE_URL))
print("[debug] SUPABASE_KEY present?", bool(SUPABASE_KEY))
print("[debug] NANSEN_API_KEY length:", len(NANSEN_API_KEY) if NANSEN_API_KEY else None)

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Try both endpoints
NANSEN_ENDPOINTS = [
    "https://api.nansen.ai/api/beta/smart-money/inflows",
    "https://api.nansen.ai/v1/smart-money/inflows"
]

def fetch_whale_flows():
    last_error = None
    for endpoint in NANSEN_ENDPOINTS:
        for headers in [
            {"Authorization": f"Bearer {NANSEN_API_KEY}", "Content-Type": "application/json"},
            {"x-api-key": NANSEN_API_KEY, "Content-Type": "application/json"}
        ]:
            try:
                print(f"[debug] Trying {endpoint} with headers {list(headers.keys())}")
                body = {
                    "parameters": {
                        "smFilter": ["180D Smart Trader", "Fund", "Smart Trader"],
                        "chains": ["ethereum", "solana"],
                        "includeStablecoin": True,
                        "includeNativeTokens": True
                    }
                }
                resp = requests.post(endpoint, headers=headers, json=body)
                if resp.status_code == 200:
                    return resp.json()
                else:
                    print(f"[debug] Failed {resp.status_code}: {resp.text[:200]}")
                    last_error = resp.text
            except Exception as e:
                last_error = str(e)
    raise RuntimeError(f"All attempts failed. Last error: {last_error}")

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
            data = fetch_whale_flows()
            upsert_whale_flows(data)
            print("✅ Done whale flows.")
        except Exception as e:
            print("❌ Error whale flows job:", e)
        time.sleep(3600)

if __name__ == "__main__":
    main()
