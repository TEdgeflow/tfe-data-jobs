import os, time, requests
from datetime import datetime, timezone
from supabase import create_client, Client

# ===== ENV VARS =====
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
NANSEN_API_KEY = os.getenv("NANSEN_API_KEY")

print("[debug] SUPABASE_URL present?", bool(SUPABASE_URL))
print("[debug] SUPABASE_KEY present?", bool(SUPABASE_KEY))
print("[debug] NANSEN_API_KEY length:", len(NANSEN_API_KEY) if NANSEN_API_KEY else None)

if not SUPABASE_URL or not SUPABASE_KEY or not NANSEN_API_KEY:
    raise RuntimeError("Missing one of SUPABASE_URL, SUPABASE_KEY, or NANSEN_API_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Try both endpoints
NANSEN_ENDPOINTS = [
    "https://api.nansen.ai/api/beta/smart-money/holdings",
    "https://api.nansen.ai/v1/smart-money/holdings"
]

def fetch_holdings():
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
                        "includeStablecoin": True
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

def upsert_holdings(data):
    rows = []
    for d in data.get("data", []):
        rows.append({
            "ts": datetime.now(timezone.utc).isoformat(),
            "token": d.get("tokenSymbol"),
            "chain": d.get("chain"),
            "holding_value": d.get("holdingUsd"),
            "holding_change": d.get("changeUsd")
        })
    if rows:
        sb.table("nansen_holdings").upsert(rows).execute()
        print(f"[upsert] {len(rows)} holdings rows")

def main():
    while True:
        try:
            data = fetch_holdings()
            upsert_holdings(data)
            print("✅ Done holdings.")
        except Exception as e:
            print("❌ Error holdings job:", e)
        time.sleep(3600)  # run every hour

if __name__ == "__main__":
    main()
