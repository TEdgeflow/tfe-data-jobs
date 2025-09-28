# ingesters/refresh_vwap.py
import os
from supabase import create_client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    raise RuntimeError("Missing Supabase credentials")

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

print("[refresh_vwap] refreshing materialized view binance_vwap_agg...")
res = supabase.rpc("refresh_binance_vwap_agg").execute()
print("[refresh_vwap] done:", res)
