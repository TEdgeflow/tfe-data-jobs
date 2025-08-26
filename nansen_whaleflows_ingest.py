import os
import time
import requests
from supabase import create_client, Client
from datetime import datetime, timezone

# ========= ENV VARS (Railway / Supabase Dashboard) =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
NANSEN_API_KEY = os.getenv("NANSEN_API_KEY")

if not SUPABASE_URL or not SUPABASE_KEY or not NANSEN_API_KEY:
    raise RuntimeError("Missing SUPABASE_URL, SUPABASE_KEY, or NANSEN_API_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ========= SETTINGS =========
TOKENS = ["eth", "btc", "sol"]   # extend this list later
CHAIN = "ethereum"               # adjust if Solana, BSC, etc.

def fetch_nansen_flows(token):
    """
    Call Nansen API for whale inflow/outflow/netflow for a token.
    """
    url = f"https://api.nansen.ai/v1/flows/{CHAIN}/{token}"
    headers = {"Authorization": f"Bearer {NANSEN_API_KEY}"}

    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        raise RuntimeError(f"Nansen API error {resp.status_code}: {resp.text}")

    data = resp.json()

    # Example structure: adapt fields depending on Nansen API response
    return {
        "symbol": token.upper(),
        "chain": CHAIN,
        "inflow": data.get("inflow", 0),
        "outflow": data.get("outflow", 0),
        "netflow": data.get("netflow", 0),
        "whale_tx_count": data.get("tx_count", 0),
        "whale_addresses": data.get("wallet_count", 0),
        "ts": datetime.now(timezone.utc).isoformat()
    }

def upsert_flows(rows):
    """
    Write rows into Supabase table whale_flows.
    """
    sb.table("whale_flows").upsert(rows).execute()
    print(f"[upsert] {len(rows)} whale flow rows…")

def main():
    print("Starting Nansen Whale Flow Job…")
    while True:
        try:
            rows = []
            for token in TOKENS:
                row = fetch_nansen_flows(token)
                rows.append(row)

            upsert_flows(rows)
            print("Done.")

        except Exception as e:
            print("Error during Nansen whale flow job:", e)

        # sleep 10 minutes (Nansen rate limits are tighter than Coingecko)
        time.sleep(600)

if __name__ == "__main__":
    main()

