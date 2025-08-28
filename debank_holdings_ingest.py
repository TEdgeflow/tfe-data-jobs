import os, time, requests
from supabase import create_client, Client
from datetime import datetime, timezone

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing Supabase ENV vars")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

DEBANK_URL = "https://openapi.debank.com/v1/user/token_list"

def fetch_debank(address: str):
    resp = requests.get(f"{DEBANK_URL}?id={address}")
    print(f"[debug] {address} -> {resp.status_code}")
    resp.raise_for_status()
    return resp.json()

def upsert_holdings(address: str, chain: str, data):
    rows = []
    for token in data:
        rows.append({
            "ts": datetime.now(timezone.utc).isoformat(),
            "address": address,
            "chain": chain,
            "token": token.get("id"),
            "balance": token.get("amount"),
            "value_usd": (token.get("price") or 0) * (token.get("amount") or 0),
            "source": "debank"
        })
    if rows:
        sb.table("whale_holdings").upsert(rows).execute()
        print(f"[upsert] {len(rows)} rows for {address}")

def main():
    while True:
        wallets = sb.table("whale_addresses").select("*").execute().data
        for w in wallets:
            try:
                data = fetch_debank(w["address"])
                upsert_holdings(w["address"], w["chain"], data)
            except Exception as e:
                print(f"❌ {w['address']} failed: {e}")
        print("✅ Cycle complete")
        time.sleep(3600)

if __name__ == "__main__":
    main()
