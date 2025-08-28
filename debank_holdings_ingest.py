import os
import time
import requests
from supabase import create_client, Client
from datetime import datetime, timezone

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ========= DeBank Public Endpoint =========
DEBANK_URL = "https://api.debank.com/token/cache_balance_list"

# Example wallet (replace with tracked addresses)
TEST_WALLET = "0x8d8dA6BF26964aF9D7eEd9e03E53415D37aA96045"  # Vitalik's

def fetch_debank_holdings(wallet):
    params = {"user_addr": wallet}
    resp = requests.get(DEBANK_URL, params=params)
    print("[debug]", resp.status_code, resp.text[:300])
    resp.raise_for_status()
    return resp.json().get("data", [])

def upsert_holdings(wallet, tokens):
    rows = []
    ts = datetime.now(timezone.utc).isoformat()
    for token in tokens:
        rows.append({
            "ts": ts,
            "wallet": wallet,
            "token": token.get("symbol"),
            "chain": token.get("chain"),
            "amount": token.get("amount"),
            "usd_value": token.get("price", 0) * token.get("amount", 0)
        })
    if rows:
        sb.table("debank_holdings").upsert(rows).execute()
        print(f"[upsert] {len(rows)} rows inserted")

def main():
    while True:
        try:
            tokens = fetch_debank_holdings(TEST_WALLET)
            upsert_holdings(TEST_WALLET, tokens)
            print("✅ Done holdings")
        except Exception as e:
            print("❌ Error in DeBank job:", e)
        time.sleep(3600)  # every 1 hour

if __name__ == "__main__":
    main()
