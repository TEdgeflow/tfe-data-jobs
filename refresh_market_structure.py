import os
from supabase import create_client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing Supabase credentials")

sb = create_client(SUPABASE_URL, SUPABASE_KEY)

print("[start] Refreshing materialized view...")

try:
    res = sb.rpc("refresh_signal_market_structure_core_raw").execute()
    print("[ok] View refreshed successfully:", res)
except Exception as e:
    print("[error] Refresh failed:", e)
