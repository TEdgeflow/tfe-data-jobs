import os
from supabase import create_client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing Supabase credentials")

sb = create_client(SUPABASE_URL, SUPABASE_KEY)

def backfill():
    print("🚀 Backfilling from binance_trades → binance_trades_agg...")
    # Fetch in batches (avoid timeouts)
    offset = 0
    batch_size = 50000
    while True:
        resp = sb.table("binance_trades") \
            .select("*") \
            .range(offset, offset + batch_size - 1) \
            .execute()

        rows = resp.data
        if not rows:
            break

        agg_map = {}
        for t in rows:
            ts = t["ts"]
            price = float(t["price"])
            qty = float(t["qty"])
            quote_qty = float(t["quote_qty"])
            side = t["side"]

            # bucket
            import datetime
            dt = datetime.datetime.fromisoformat(ts.replace("Z", "+00:00"))
            bucket_15m = dt.replace(minute=(dt.minute // 15) * 15, second=0, microsecond=0)
            bucket_1h = dt.replace(minute=0, second=0, microsecond=0)
            bucket_1d = dt.replace(hour=0, minute=0, second=0, microsecond=0)

            key = (t["symbol"], bucket_15m, bucket_1h, bucket_1d)
            if key not in agg_map:
                agg_map[key] = {
                    "symbol": t["symbol"],
                    "bucket_15m": bucket_15m.isoformat(),
                    "bucket_1h": bucket_1h.isoformat(),
                    "bucket_1d": bucket_1d.isoformat(),
                    "buy_vol": 0,
                    "sell_vol": 0,
                    "delta": 0,
                    "bullish_trades": 0,
                    "bearish_trades": 0,
                }

            row = agg_map[key]
            if side.upper() == "BUY":
                row["buy_vol"] += quote_qty
                row["delta"] += quote_qty
                row["bullish_trades"] += 1
            else:
                row["sell_vol"] += quote_qty
                row["delta"] -= quote_qty
                row["bearish_trades"] += 1

        # naive CVD = delta per bucket (extend later for cumulative)
        for row in agg_map.values():
            row["cvd"] = row["delta"]

        sb.table("binance_trades_agg").upsert(list(agg_map.values())).execute()
        print(f"✅ Upserted {len(agg_map)} rows (offset {offset})")

        offset += batch_size

if __name__ == "__main__":
    backfill()
