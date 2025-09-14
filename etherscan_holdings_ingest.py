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

# 👉 Add whale wallets you want to track here
WALLETS = [
    "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",  # Vitalik
    # add more whale wallets here
]

def fetch_debank_holdings(wallet, retries=5):
    """Fetch token balances from DeBank with retry on 429 (rate limit)."""
    params = {"user_addr": wallet}
    for attempt in range(retries):
        resp = requests.get(DEBANK_URL, params=params)
        if resp.status_code == 200:
            data = resp.json().get("data", [])
            print(f"✅ {wallet[:6]}... fetched {len(data)} tokens")
            return data
        elif resp.status_code == 429:  # Too many requests
            wait = (2 ** attempt) * 5  # exponential backoff: 5s, 10s, 20s...
            print(f"⚠️ Rate limited for {wallet[:6]}..., waiting {wait}s before retry...")
            time.sleep(wait)
        else:
            print(f"❌ Error {resp.status_code} for {wallet}: {resp.text[:200]}")
            return []
    return []

def upsert_whale_flows(wallet, tokens):
    """Insert balances into whale_flows table."""
    rows = []
    ts = datetime.now(timezone.utc).isoformat()
    
    for token in tokens:
        symbol = token.get("symbol")
        amount = token.get("amount", 0)
        usd_value = token.get("price", 0) * amount

        rows.append({
            "token_symbol": symbol,
            "inflow_usd": usd_value,         # treating holding as inflow snapshot
            "outflow_usd": 0,                # DeBank API doesn’t give outflow directly
            "netflow_usd": usd_value,        # inflow - outflow
            "whale_percent": None,           # optional if you calculate later
            "source": "debank",
            "recorded_at": ts
        })

    if rows:
        sb.table("whale_flows").insert(rows).execute()
        print(f"[insert] {len(rows)} tokens saved for {wallet[:6]}... → whale_flows")
    else:
        print(f"⚠️ No rows to insert for {wallet[:6]}...")

def main():
    while True:
        print("🚀 Starting DeBank whale_flows fetch...")
        for wallet in WALLETS:
            try:
                tokens = fetch_debank_holdings(wallet)
                upsert_whale_flows(wallet, tokens)
                time.sleep(10)  # ⏳ delay between wallets to avoid 429
            except Exception as e:
                print(f"❌ Error processing {wallet}: {e}")
        print("✅ Cycle complete. Sleeping 1 hour...\n")
        time.sleep(3600)

if __name__ == "__main__":
    main()


