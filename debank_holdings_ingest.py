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

# ========= DeBank OpenAPI =========
DEBANK_URL = "https://openapi.debank.com/v1/user/token_list"

# 👉 Replace with a wallet you want to track (test with Vitalik's wallet first)
TEST_WALLET = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"


def fetch_debank_holdings(wallet):
    url = f"{DEBANK_URL}?id={wallet}"
    resp = requests.get(url)
    print("[debug] status", resp.status_code)
    resp.raise_for_status()
    return resp.json()


def upsert_holdings(wallet, data):
    rows = []
    ts = datetime.now(timezone.utc).isoformat()
    for token in data:
        rows.append({
            "ts": ts,
            "wallet": wallet,
            "chain": token.get("chain"),
            "token": token.get("id"),
            "symbol": token.get("symbol"),
            "balance": token.get("amount"),
            "usd_value": token.get("price", 0) * token.get("amount", 0)
        })

    if rows:
        sb.table("debank_holdings").upsert(rows).execute()
        print(f"[upsert] {len(rows)} holdings rows")


def main():
    while True:
        try:
            print(f"Fetching DeBank holdings for {TEST_WALLET}...")
            data = fetch_debank_holdings(TEST_WALLET)
            upsert_holdings(TEST_WALLET, data)
            print("✅ Done DeBank holdings.")
        except Exception as e:
            print("❌ Error in DeBank job:", e)

        time.sleep(3600)  # run every hour


if __name__ == "__main__":
    main()
