import os, csv, io, requests
from datetime import datetime, timezone
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
CSV_URL = os.getenv("UNLOCKS_CSV_URL")  # set in Railway to your raw GitHub CSV url

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY")
if not CSV_URL:
    raise RuntimeError("Missing UNLOCKS_CSV_URL")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
ALLOWED_TYPES = {"CLIFF","LINEAR","OTHER"}

def normalize_row(row: dict) -> dict:
    ts = (row.get("unlock_time") or "").strip()
    if ts.endswith("Z"):
        iso = ts
    else:
        iso = datetime.fromisoformat(ts).astimezone(timezone.utc).isoformat()

    t = (row.get("unlock_type") or "OTHER").upper().strip()
    if t not in ALLOWED_TYPES: t = "OTHER"

    def num(x):
        if x in (None, "", "null", "NaN"): return None
        try: return float(x)
        except: return None

    return {
        "symbol": (row.get("symbol") or "").upper().strip(),
        "unlock_time": iso,
        "unlock_type": t,
        "amount_tokens": num(row.get("amount_tokens")),
        "amount_usd": num(row.get("amount_usd")),
        "pct_circ": num(row.get("pct_circ")),
        "source": (row.get("source") or "manual").strip(),
    }

def run():
    r = requests.get(CSV_URL, timeout=25); r.raise_for_status()
    reader = csv.DictReader(io.StringIO(r.text))
    batch = []
    for raw in reader:
        n = normalize_row(raw)
        if not n["symbol"] or not n["unlock_time"]: continue
        batch.append(n)

    if batch:
        sb.table("token_unlocks").upsert(
            batch,
            on_conflict="symbol,unlock_time,unlock_type,source"
        ).execute()
        print(f"[unlocks] upserted {len(batch)} rows")
    else:
        print("[unlocks] nothing to upsert")

if __name__ == "__main__":
    run()

