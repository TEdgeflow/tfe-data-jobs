import os
from supabase import create_client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

sb = create_client(SUPABASE_URL, SUPABASE_KEY)

views = [
    "binance_orderbook_agg_min",
    "binance_orderbook_agg_5m",
    "binance_orderbook_agg_15m",
    "binance_orderbook_agg_1h",
    "binance_orderbook_agg_4h",
    "binance_orderbook_depth_1m",
    "binance_orderbook_depth_5m",
    "binance_orderbook_depth_15m",
    "binance_orderbook_depth_1h",
    "binance_orderbook_depth_4h",
]

for v in views:
    print(f"[refresh_orderbook] refreshing {v}...")
    try:
        res = sb.rpc("refresh_materialized_view", {"view_name": v}).execute()
        print(f"[refresh_orderbook] ✅ {v} refreshed:", res)
    except Exception as e:
        print(f"[refresh_orderbook] ❌ failed for {v}: {e}")

