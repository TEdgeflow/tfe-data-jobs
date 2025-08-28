import os
import time
import requests
from supabase import create_client, Client
from datetime import datetime, timezone

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
ETHERSCAN_API = os.getenv("ETHERSCAN_API")

if not SUPABASE_URL or not SUPABASE_KEY or not ETHERSCAN_API:
    raise RuntimeError("Missing SUPABASE_URL, SUPABASE_KEY, or ETHERSCAN_API")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ========= Etherscan Endpoint =========
ETHERSCAN_URL = "https://api.etherscan.io/api"

# 👉 Add wallets to track
WALLETS = [
    "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",  # Vitalik
    # add more
]

def fetch_eth_balance(wallet):
    """Fetch ETH balance in Wei, convert to ETH"""
    params = {
        "module": "account",
        "action": "balance",
        "address": wallet,
        "tag": "latest",
        "apikey": ETHERSCAN_API
    }
    resp = requests.get(ETHERSCAN_URL, params=params)
    resp.raise_for_status()
    data = resp.json()
    if data.get("status") == "1":
        balance_wei = int(data["result"])
        balance_eth = balance_wei / 10**18
        print(f"✅ {wallet[:6]}... ETH balance: {balance_eth:.4f}")
        return balance_eth
    else:
        print(f"❌ Failed for {wallet}: {data}")
        return None

def upsert_eth_balance(wallet, balance):
    """Insert ETH balance into Supabase"""
    if balance is None:
        return
    row = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "wallet": wallet,
        "token": "ETH",
        "chain": "ethereum",
        "amount": balance,
        "usd_value": None  # you can join with Coingecko price later
    }
    sb.table("etherscan_holdings").upsert([row]).execute()
    print(f"[upsert] ETH balance saved for {wallet[:6]}...")

def main():
    while True:
        print("🚀 Starting Etherscan balances fetch...")
        for wallet in WALLETS:
            try:
                balance = fetch_eth_balance(wallet)
                upsert_eth_balance(wallet, balance)
                time.sleep(5)  # ⏳ small delay between wallets
            except Exception as e:
                print(f"❌ Error processing {wallet}: {e}")
        print("✅ Cycle complete. Sleeping 1 hour...\n")
        time.sleep(3600)

if __name__ == "__main__":
    main()
