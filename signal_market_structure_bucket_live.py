import os
import time
from datetime import datetime, timezone
from supabase import create_client, Client
import httpx
from requests.exceptions import Timeout

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing Supabase credentials")

# ✅ Manual Supabase Client (SDK v2+ compatible)
from gotrue import SyncGoTrueClient
from postgrest import SyncPostgrestClient
from storage3 import SyncStorageClient
from supabase._sync.client import SyncClient

sb = SyncClient(
    supabase_url=SUPABASE_URL,
    supabase_key=SUPABASE_KEY,
    auth_client=SyncGoTrueClient(f"{SUPABASE_URL}/auth/v1", SUPABASE_KEY),
    storage_client=SyncStorageClient(f"{SUPABASE_URL}/storage/v1", SUPABASE_KEY),
    postgrest_client=SyncPostgrestClient(f"{SUPABASE_URL}/rest/v1", SUPABASE_KEY),
)

# ✅ 60-second RPC timeout (to prevent query freezes)
sb.postgrest_client.session.timeout = 60

# ========= CONFIG =========
TIMEFRAMES = ["15m", "1h", "1d"]
TIME_WINDOW_HOURS = {
    "15m": 3,   # pull last 3 h
    "1h": 12,   # pull last 12 h
    "1d": 72    # pull last 3 days
}
LIMIT_ROWS = 5000
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds between retries
TABLE_NAME = "signal_market_structure_agg_tf"

# ========= QUERY BUILDER =========
def build_query(tf: str, window_hours: int):
    return f"""
    with core as (
        select
            d.symbol,
            '{tf}' as timeframe,
            d.signal_time,
            d.strength_value as delta_strength,
            d.direction as delta_direction,
            c.strength_value as cvd_strength,
            c.direction as cvd_direction,
            v.vwap_dist_pct as vwap_deviation,
            f.funding_rate,
            o.open_interest,
            p.close as price_close,
            p.volume as trade_volume,
            r.rsi_14,
            r.stoch_rsi_k,
            r.stoch_rsi_d
        from v_signal_delta_{tf} d
        left join v_signal_cvd_{tf} c on c.symbol = d.symbol and c.signal_time = d.signal_time
        left join v_vwap_{tf} v on v.symbol = d.symbol
        left join v_funding_rates f on f.symbol = d.symbol
        left join v_open_interest o on o.symbol = d.symbol
        left join binance_ohlcv p on p.symbol = d.symbol and p.interval = '1h'
        left join v_ai_signal_rsi r on r.symbol = d.symbol and r.timeframe = '1d'
        where d.signal_time > now() - interval '{window_hours} hours'
        order by d.signal_time desc
        limit {LIMIT_ROWS}
    )
    select * from core;
    """

# ========= FETCH + UPSERT =========
def fetch_and_upsert(tf: str):
    q = build_query(tf, TIME_WINDOW_HOURS[tf])
    attempt = 0

    while attempt < MAX_RETRIES:
        try:
            try:
                res = sb.rpc("exec_sql", {"sql": q}).execute()
            except Timeout:
                print(f"[error:{tf}] RPC timeout — skipping timeframe.")
                return 0

            data = res.data or []
            if not data:
                print(f"[skip:{tf}] No data returned.")
                return 0

            now_ts = datetime.now(timezone.utc).isoformat()
            for r in data:
                r["last_updated_at"] = now_ts

            allowed = [
                "symbol", "timeframe", "signal_time",
                "delta_strength", "delta_direction",
                "cvd_strength", "cvd_direction",
                "vwap_deviation", "funding_rate",
                "open_interest", "price_close", "trade_volume",
                "rsi_14", "stoch_rsi_k", "stoch_rsi_d",
                "last_updated_at"
            ]
            filtered = [{k: v for k, v in r.items() if k in allowed} for r in data]

            sb.table(TABLE_NAME).upsert(
                filtered, on_conflict=["symbol", "timeframe", "signal_time"]
            ).execute()

            print(f"[ok:{tf}] Upserted {len(filtered)} rows to {TABLE_NAME}")
            return len(filtered)

        except Exception as e:
            attempt += 1
            print(f"[error:{tf}] Attempt {attempt}/{MAX_RETRIES} failed: {e}")
            if attempt < MAX_RETRIES:
                print(f"→ Retrying in {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY)
            else:
                print(f"[fail:{tf}] Giving up after {MAX_RETRIES} attempts.")
                return 0

# ========= MAIN =========
def main():
    print(f"[start] Market structure TF aggregation at {datetime.now(timezone.utc).isoformat()}")
    print("[debug] using on_conflict = ['symbol', 'timeframe', 'signal_time']")

    total_rows = 0
    for tf in TIMEFRAMES:
        print(f"----- Processing {tf} -----")
        rows = fetch_and_upsert(tf)
        total_rows += rows
        print(f"[summary:{tf}] {rows} rows processed\n")

    print("========== SUMMARY ==========")
    print(f"Total rows upserted across TFs: {total_rows}")
    print("================================")
    print("[complete] TF aggregation finished.")

if __name__ == "__main__":
    main()




