import os
import sys
from supabase import create_client
from datetime import datetime

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("[error] Missing Supabase credentials")
    sys.exit(1)

sb = create_client(SUPABASE_URL, SUPABASE_KEY)

# ========= SIGNAL VIEWS =========
SIGNAL_VIEWS = [
    "v_signal_funding_squeeze",   # Funding + OI + Volume + MC (short-term)
    # "v_signal_unlock_risk",      # <- add once created
    # "v_signal_liquidity_trap",   # <- add once created
    # "v_signal_liquidation_cluster" # <- add once created
]

def fetch_and_log_signals():
    for view in SIGNAL_VIEWS:
        print(f"[boot] Fetching signals from {view}...")

        query = f"""
            select signal_id, symbol, signal_type, signal_category,
                   confidence_score, signal_strength, rationale, created_at
            from {view}
            where created_at > now() - interval '1 hour';
        """

        try:
            result = sb.rpc("exec_sql", {"query": query}).execute()
        except Exception as e:
            print(f"[error] Failed fetching from {view}: {e}")
            continue

        if not result.data:
            print(f"[info] No new signals from {view}")
            continue

        for row in result.data:
            try:
                sb.table("ai_signals").insert(row).execute()
                print(f"[ok] Inserted {row['signal_type']} for {row['symbol']}")
            except Exception as e:
                print(f"[skip] Could not insert {row}: {e}")

if __name__ == "__main__":
    fetch_and_log_signals()
