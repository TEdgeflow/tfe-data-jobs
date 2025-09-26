import os
import time
from supabase import create_client, Client
from datetime import datetime

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10000"))
JOB_NAME = "binance_trades_backfill"

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_last_offset():
    resp = sb.table("backfill_progress").select("last_offset").eq("job_name", JOB_NAME).execute()
    if resp.data:
        return resp.data[0]["last_offset"]
    return 0

def update_offset(offset: int):
    sb.table("backfill_progress").upsert({
        "job_name": JOB_NAME,
        "last_offset": offset
    }).execute()

def get_total_rows():
    resp = sb.rpc("count_rows", {"table_name": "binance_trades"}).execute()
    return resp.data if resp.data else 0

def backfill():
    last_offset = get_last_offset()
    print(f"🚀 Resuming backfill from offset {last_offset}")

    while True:
        # Pull batch
        rows = sb.table("binance_trades") \
            .select("*") \
            .range(last_offset, last_offset + BATCH_SIZE - 1) \
            .execute()

        if not rows.data:
            print("✅ Backfill complete")
            break

        # Insert into agg table
        for row in rows.data:
            sb.table("binance_trades_agg").upsert({
                "symbol": row["symbol"],
                "bucket_1h": row["ts"],
                "delta": float(row["qty"]) if row["side"] == "BUY" else -float(row["qty"]),
                "cvd": 0,  # update later
                "bullish_trades": 1 if row["side"] == "BUY" else 0,
                "bearish_trades": 1 if row["side"] == "SELL" else 0,
            }).execute()

        last_offset += BATCH_SIZE
        update_offset(last_offset)
        print(f"✅ Upserted {len(rows.data)} rows (offset {last_offset})")

        time.sleep(0.2)  # prevent overload

if __name__ == "__main__":
    backfill()

