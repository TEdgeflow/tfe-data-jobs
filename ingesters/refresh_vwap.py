# ingesters/refresh_vwap.py
import os
from supabase import create_client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")  # using SUPABASE_KEY just like your other ingesters

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing Supabase credentials")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

print("[refresh_vwap] refreshing materialized view binance_vwap_agg...")
res = supabase.rpc("refresh_binance_vwap_agg").execute()
print("[refresh_vwap] done:", res)

# 🔹 Extra check: fetch last 5 rows to confirm update
latest = supabase.table("binance_vwap_agg") \
    .select("symbol, timeframe, bucket_start") \
    .order("bucket_start", desc=True) \
    .limit(5) \
    .execute()

print("[refresh_vwap] latest rows:", latest.data)

