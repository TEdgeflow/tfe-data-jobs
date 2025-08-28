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

# 👉 Add wallets you want to track
WALLETS = [
    "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",  # Vitalik
    # add more whale wallets here
]


def fetch_tracked_tokens():
    """Fetch token list from Supabase table tracked_tokens."""
    resp = sb.table("tracked_tokens").select("*").execute()
    tokens = {row["symbol"]: (row["contract_address"], row["decimals"]) for row in resp.data}
    print(f"📌 Loaded {len(tokens)} tokens from Supabase: {list(tokens.keys())}")
    return tokens


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
        return balance_eth
    return None


def fetch_token_balance(wallet, token_name, contract_address, decimals=18):
    """Fetch ERC-20 token balance for a wallet"""
    params = {
        "module": "account",
        "action": "tokenbalance",
        "contractaddress": contract_address,
        "address": wallet,
        "tag": "latest",
        "apikey": ETHERSCAN_API
    }
    resp = requests.get(ETHERSCAN_URL, params=params)
    resp.raise_for_status()
    data = resp.json()
    if data.get("status") == "1":
        raw_balance = int(data["result"])
        balance = raw_balance / (10 ** decimals)
        return balance
    return None


def upsert_balance(wallet, token, chain, amount):
    """Insert balance into Supabase"""
    if amount is None:
        return
    row = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "wallet": wallet,
        "token": token,
        "chain": chain,
        "amount": amount,
        "usd_value": None  # later enrich with Coingecko
    }
    sb.table("etherscan_holdings").upsert([row]).execute()
    print(f"[upsert] {token} balance saved for {wallet[:6]}... ({amount})")


def main():
    while True:
        print("🚀 Starting Etherscan balances fetch...")
        tokens = fetch_tracked_tokens()

        for wallet in WALLETS:
            try:
                # ETH balance
                eth_balance = fetch_eth_balance(wallet)
                upsert_balance(wallet, "ETH", "ethereum", eth_balance)
                print(f"✅ {wallet[:6]}... ETH: {eth_balance:.4f}")

                # ERC20 tokens from Supabase list
                for token, (contract, decimals) in tokens.items():
                    bal = fetch_token_balance(wallet, token, contract, decimals)
                    upsert_balance(wallet, token, "ethereum", bal)
                    print(f"   {token}: {bal}")

                time.sleep(10)  # delay between wallets

            except Exception as e:
                print(f"❌ Error processing {wallet}: {e}")

        print("✅ Cycle complete. Sleeping 1 hour...\n")
        time.sleep(3600)


if __name__ == "__main__":
    main()

