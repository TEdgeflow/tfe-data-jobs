import os, time, requests
from datetime import datetime, timezone
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
LUNAR_API_KEY = os.getenv("LUNAR_API_KEY")

print("[debug] SUPABASE_URL =", SUPABASE_URL)
print("[debug] SUPABASE_KEY present?", bool(SUPABASE_KEY))
print("[debug] LUNAR_API_KEY present?", bool(LUNAR_API_KEY))

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE_URL = "https://lunarcrush.com/api4/public"

def fetch_mentions(limit=20):
    url = f"{BASE_URL}/mentions/v1?limit={limit}&sort=interactions_24h&desc=true"
    headers = {"Authorization": f"Bearer {LUNAR_API_KEY}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()

def upsert_mentions(data):
    rows = []
    for d in data.get("data", []):
        rows.append({
            "ts": datetime.now(timezone.utc).isoformat(),
            "symbol": d.get("s"),
            "mentions_count": d.get("mc"),
            "interactions_24h": d.get("i24h")
        })
    if rows:
        sb.table("social_mentions").upsert(rows).execute()
        print(f"[✅] Upserted {len(rows)} mentions rows")

def main():
    while True:
        try:
            data = fetch_mentions(limit=20)
            upsert_mentions(data)
        except Exception as e:
            print("❌ Mentions error:", e)
        time.sleep(300)  # every 5 min

if __name__ == "__main__":
    main()
