# ingesters/refresh_orderbook.py
import os
from supabase import create_client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing Supabase credentials")

sb = create_client(SUPABASE_URL, SUPABASE_KEY)

# List of orderbook views to refresh
VIEWS = [
    "binance_orderbook_agg_5m",
    "binance_orderbook_agg_15m",
    "binance_orderbook_agg_1h",
    "binance_orderbook_agg_4h",
    "binance_orderbook_depth_5m",
    "binance_orderbook_depth_15m",
    "binance_orderbook_depth_1h",
    "binance_orderbook_depth_4h",
]

for v in VIEWS:
    print(f"[refresh_orderbook] refreshing {v}...")
    res = sb.rpc(f"refresh_{v}").execute()
    print(f"[refresh_orderbook] {v} done: {res}")
