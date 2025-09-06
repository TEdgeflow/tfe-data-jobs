import os
import requests
from datetime import datetime, timezone
from supabase import create_client, Client

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DROPTABS_KEY = os.getenv("DROPTABS_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("❌ Missing Supabase credentials")

if not DROPTABS_KEY:
    raise RuntimeError("❌ Missing Droptabs API key")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE_URL = "https://public-api.dropstab.com/api/v1"

# ========= HELPERS =========
def iso_now():
    return datetime.now(timezone.utc).isoformat()

def fetch_api(endpoint, params=None):
    """Fetch JSON from Dropstab API with api_key (query + header)."""
    url = f"{BASE_URL}/{endpoint}"
    params = params or {}
    params["api_key"] = DROPTABS_KEY   # ✅ query param

    headers = {
        "accept": "application/json",
        "x-dropstab-api-key": DROPTABS_KEY   # ✅ header fallback
    }

    print(f"🔗 Fetching {url} with params {params}")
    resp = requests.get(url, params=params, headers=headers)
    resp.raise_for_status()
    return resp.json()

def upsert(table, rows):
    if not rows:
        print(f"⚠️ No rows to upsert into {table}")
        return
    try:
        sb.table(table).upsert(rows).execute()
        print(f"⬆️ Upserted {len(rows)} rows into {table}")
    except Exception as e:
        print(f"❌ Supabase insert failed for {table}: {e}")

# ========= INGESTION =========
def ingest_supported_coins():
    data = fetch_api("tokenUnlocks/supportedCoins", {"pageSize": 50, "page": 0})
    if not data:
        return

    rows = []
    for c in data.get("data", []):
        if isinstance(c, dict):
            rows.append({
                "slug": c.get("slug"),
                "symbol": c.get("symbol"),
                "name": c.get("name"),
                "last_update": iso_now()
            })
        elif isinstance(c, str):
            rows.append({
                "slug": c,
                "symbol": None,
                "name": None,
                "last_update": iso_now()
            })

    upsert("droptabs_supported_coins", rows)

def ingest_unlocks():
    data = fetch_api("tokenUnlocks", {"pageSize": 50, "page": 0})
    if not data:
        return

    rows = []
    for u in data.get("data", []):
        coin = u.get("coin", {}) if isinstance(u.get("coin"), dict) else {}
        rows.append({
            "coin_id": u.get("coinId"),
            "coin_slug": coin.get("slug"),
            "coin_symbol": coin.get("symbol"),
            "unlock_date": u.get("date"),
            "amount": u.get("amount"),
            "category": u.get("category"),
            "last_update": iso_now()
        })

    upsert("droptabs_unlocks", rows)

# ========= MAIN =========
def run_all():
    print("🚀 Starting Droptabs ingestion...")
    ingest_supported_coins()
    ingest_unlocks()
    print("✅ Finished Droptabs ingestion.")

if __name__ == "__main__":
    run_all()




