# ingest_extras.py
import os, time
from supabase import create_client
from datetime import datetime, timezone

from ingesters.unlocks import fetch_unlocks_for
from ingesters.heatmap import fetch_binance_depth, bucketize_depth
from ingesters.maxpain import compute_max_pain_for_exp
import requests

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
sb = create_client(SUPABASE_URL, SUPABASE_KEY)

def upsert(table: str, rows: list, conflict_cols: list):
    if rows:
        sb.table(table).upsert(rows, on_conflict=",".join(conflict_cols)).execute()

def ingest_unlocks(symbols: list[str]):
    rows = []
    for s in symbols:
        try:
            rows += fetch_unlocks_for(s)
            time.sleep(0.2)
        except Exception as e:
            print("[unlocks]", s, e)
    upsert("token_unlocks", rows, ["symbol","unlock_time","unlock_type","source"])

def ingest_heatmap(symbols: list[str], venue="binance"):
    for s in symbols:
        try:
            depth = fetch_binance_depth(s, limit=1000)
            buckets = bucketize_depth(s, venue, depth, bucket_bps=10.0)
            # Write one timestamp snapshot per symbol
            upsert("liquidity_heatmap", buckets, ["ts","venue","symbol","price_level","side"])
            time.sleep(0.15)
        except Exception as e:
            print("[heatmap]", s, e)

def ingest_maxpain(currency_list=("BTC","ETH")):
    from datetime import date
    rows = []
    for cur in currency_list:
        # Example: compute today’s nearest monthly expiries – real logic: list chain expiries and loop
        for d in ():
            pass  # Placeholder if you want to loop expiries
    # Example for a single expiration call:
    # result = compute_max_pain_for_exp(currency="BTC", expiration=date(2025,8,29))
    # if result: rows.append(result)
    # When you have rows:
    # upsert("options_metrics", rows, ["token","venue","expiration"])

if __name__ == "__main__":
    # pick a small subset first
    symbols = ["BTCUSDT","ETHUSDT"]
    ingest_unlocks(symbols)
    ingest_heatmap(symbols)
    # ingest_maxpain()  # enable once you finalize expiration selection
    print("extra ingestion done.")
