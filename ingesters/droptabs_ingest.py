import os
import requests
from datetime import datetime, timezone
from supabase import create_client, Client

# ========= ENV VARS (Railway) =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ========= API URLs =========
SUPPORTED_COINS_URL = "https://public-api.dropstab.com/api/v1/tokenUnlocks/supportedCoins"
UNLOCKS_URL = "https://public-api.dropstab.com/api/v1/tokenUnlocks"


def ingest_supported_coins():
    print("🔗 Fetching supported coins...")
    resp = requests.get(SUPPORTED_COINS_URL)
    data = resp.json()

    rows = []
    for c in data:
        # Some APIs return dicts, others strings → normalize
        if isinstance(c, dict):
            rows.append({
                "symbol": c.get("symbol"),
                "slug": c.get("slug"),
                "name": c.get("name"),
                "id": c.get("id"),
                # Uncomment if you actually added this column in Supabase
                # "last_update": datetime.now(timezone.utc).isoformat(),
            })
        else:
            rows.append({
                "symbol": str(c),
                "slug": None,
                "name": None,
                "id": None,
            })

    if rows:
        try:
            sb.table("droptabs_supported_coins").upsert(rows).execute()
            print(f"✅ Inserted {len(rows)} supported coins")
        except Exception as e:
            print("❌ Supabase insert failed for droptabs_supported_coins:", e)


def ingest_unlocks():
    print("🔗 Fetching unlocks...")
    resp = requests.get(UNLOCKS_URL)
    data = resp.json()

    rows = []
    for u in data:
        coin_data = u.get("coin")
        if isinstance(coin_data, dict):
            coin_symbol = coin_data.get("symbol")
        else:
            coin_symbol = coin_data  # fallback if string

        rows.append({
            "coin": coin_symbol,
            "amount": u.get("amount"),
            "unlock_date": u.get("date"),
            "category": u.get("category"),
            # "last_update": datetime.now(timezone.utc).isoformat(),  # only if column exists
        })

    if rows:
        try:
            sb.table("droptabs_unlocks").upsert(rows).execute()
            print(f"✅ Inserted {len(rows)} unlock records")
        except Exception as e:
            print("❌ Supabase insert failed for droptabs_unlocks:", e)


def run_all():
    ingest_supported_coins()
    ingest_unlocks()


if __name__ == "__main__":
    print("🚀 Starting Droptabs ingestion...")
    run_all()







