import os, time, requests
from supabase import create_client, Client
from datetime import datetime, timezone

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
ETHERSCAN_API = os.getenv("ETHERSCAN_API")  # add to Railway variables

if not SUPABASE_URL or not SUPABASE_KEY or not ETHERSCAN_API:
    raise RuntimeError("Missing ENV vars")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

ETHERSCAN_BASE = "https://api.etherscan.io/api"

def fetch_token_balance(token_addr, wallet):
    url = f"{ETHERSCAN_BASE}?module=account&action=tokenbalance&contractaddress={token_addr}&address={wallet}&tag=latest&apikey={ETHERSCAN_API}"
    resp = requests.get(url)
    resp.raise_for_status()
    return int(resp.json()["result"]) / 1e18

def upsert_balance(address, chain, token, balance):
    row = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "address": address,
        "chain": chain,
        "token": token,
        "balance": balance,
        "value_usd": None,   # you can enrich with Coingecko price later
        "source": "etherscan"
    }
    sb.table("whale_holdings").upsert(row).execute()
    print(f"[upsert] {token} for {address}")

def main():
    while True:
        wallets = sb.table("whale_addresses").select("*").execute().data
        for w in wallets:
            # TODO: you must know the token contract addresses here
            pass
        print("✅ Etherscan cycle done")
        time.sleep(7200)

if __name__ == "__main__":
    main()
