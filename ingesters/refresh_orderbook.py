# ingesters/refresh_orderbook.py
import os
from supabase import create_client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing Supabase credentials")

sb = create_client(SUPABASE_URL, SUPABASE_KEY)

# list of all views to refresh
VIEWS = [
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

def refresh_view(view):
    print(f"[refresh_orderbook] refreshing {view}...")
    try:
        res = sb.rpc(f"refresh_{view}").execute()
        print(f"[refresh_orderbook] ✅ {view} refreshed: {res}")
    except Exception as e:
        print(f"[refresh_orderbook] ❌ failed to refresh {view}: {e}")

if __name__ == "__main__":
    for v in VIEWS:
        refresh_view(v)
