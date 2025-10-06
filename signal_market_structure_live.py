import os
import time
from datetime import datetime, timezone
from supabase import create_client, Client

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing Supabase credentials")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ========= CONFIG =========
TIME_WINDOW_HOURS = int(os.getenv("TIME_WINDOW_HOURS", "6"))  # recent window
LIMIT_ROWS = int(os.getenv("LIMIT_ROWS", "500"))
RETRY_LIMIT = 3

# ========= FETCH RECENT SIGNALS =========
def fetch_recent_signals():
    query = f"""
    with core as (
      select
        d.symbol,
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
      from v_signal_delta_5m d
      left join v_signal_cvd_5m c on c.symbol = d.symbol and c.signal_time = d.signal_time
      left join v_vwap_5m v on v.symbol = d.symbol
      left join v_funding_rates f on f.symbol = d.symbol
      left join v_open_interest o on o.symbol = d.symbol
      left join binance_ohlcv p on p.symbol = d.symbol and p.interval = '1h'
      left join v_ai_signal_rsi r on r.symbol = d.symbol and r.timeframe = '1d'
      where d.signal_time > now() - interval '{TIME_WINDOW_HOURS} hours'
      order by d.signal_time desc
      limit {LIMIT_ROWS}
    )
    select * from core;
    """

    try:
        res = sb.rpc("exec_sql", {"sql": query}).execute()
        if not res.data:
            print("[skip] No new signals found in recent window.")
            return []
        print(f"[fetch] Retrieved {len(res.data)} live records.")
        return res.data
    except Exception as e:
        print(f"[error] Failed fetching recent data: {e}")
        return []

# ========= UPSERT DATA =========
def upsert_live_signals(data):
    if not data:
        print("[skip] No new data to insert.")
        return

    now_ts = datetime.now(timezone.utc).isoformat()
    for row in data:
        row["last_updated_at"] = now_ts

    allowed_columns = [
        "symbol", "signal_time", "delta_strength", "delta_direction",
        "cvd_strength", "cvd_direction", "vwap_deviation",
        "funding_rate", "open_interest", "price_close",
        "trade_volume", "rsi_14", "stoch_rsi_k", "stoch_rsi_d", "last_updated_at"
    ]
    filtered_data = [{k: v for k, v in row.items() if k in allowed_columns} for row in data]

    for attempt in range(RETRY_LIMIT):
        try:
            sb.table("signal_market_structure_core_raw").upsert(
                filtered_data,
                on_conflict=["symbol", "signal_time"]
            ).execute()
            print(f"[ok] Live ingestion upserted {len(filtered_data)} rows.")
            return
        except Exception as e:
            print(f"[warn] Attempt {attempt+1} failed: {e}")
            time.sleep(3)
    print("[error] All retries failed during live upsert.")

# ========= REFRESH MATERIALIZED VIEW =========
def refresh_materialized_view():
    try:
        print("[refresh] Refreshing materialized view v_signal_market_structure_core_mat...")
        sb.rpc("exec_sql", {"sql": "REFRESH MATERIALIZED VIEW CONCURRENTLY v_signal_market_structure_core_mat;"}).execute()
        print("[ok] Materialized view refreshed successfully.")
    except Exception as e:
        print(f"[warn] Failed to refresh materialized view: {e}")

# ========= MAIN LOOP =========
def main():
    print(f"[start] Live ingestion started at {datetime.now().isoformat()}")
    data = fetch_recent_signals()
    upsert_live_signals(data)
    refresh_materialized_view()
    print(f"[done] Processed {len(data)} live records.")

if __name__ == "__main__":
    main()
