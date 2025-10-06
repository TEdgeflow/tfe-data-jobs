import os
from datetime import datetime, timezone
from supabase import create_client, Client
import time

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing Supabase credentials")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ========= CONFIG =========
TIMEFRAMES = ["15m", "1h", "4h", "1d"]
LIMIT_ROWS = 500
REFRESH_VIEW = "v_signal_market_structure_core_mat"

# ========= QUERY TEMPLATE =========
def build_query(tf: str) -> str:
    """Return SQL query customized per timeframe."""
    return f"""
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
      from v_signal_delta_{tf} d
      left join v_signal_cvd_{tf} c on c.symbol = d.symbol and c.signal_time = d.signal_time
      left join v_vwap_5m v on v.symbol = d.symbol  -- keep 5m VWAP baseline
      left join v_funding_rates f on f.symbol = d.symbol
      left join v_open_interest o on o.symbol = d.symbol
      left join binance_ohlcv p on p.symbol = d.symbol and p.interval = '1h'
      left join v_ai_signal_rsi r on r.symbol = d.symbol and r.timeframe = '1d'
      where d.signal_time > now() - interval '72 hours'
      limit {LIMIT_ROWS}
    )
    select *, '{tf}' as timeframe from core;
    """

# ========= FETCH FUNCTION =========
def fetch_combined_data(tf):
    try:
        query = build_query(tf)
        res = sb.rpc("exec_sql", {"sql": query}).execute()
        if not res.data:
            print(f"[skip:{tf}] No data fetched.")
            return []
        print(f"[fetch:{tf}] Retrieved {len(res.data)} rows.")
        return res.data
    except Exception as e:
        print(f"[error:{tf}] Fetch failed: {e}")
        return []

# ========= UPSERT FUNCTION =========
def upsert_signal_data(data):
    if not data:
        print("[skip] No new data to upsert.")
        return

    now_ts = datetime.now(timezone.utc).isoformat()
    for row in data:
        row["last_updated_at"] = now_ts

    allowed_columns = [
        "symbol", "signal_time", "delta_strength", "delta_direction",
        "cvd_strength", "cvd_direction", "vwap_deviation",
        "funding_rate", "open_interest", "price_close", "trade_volume",
        "rsi_14", "stoch_rsi_k", "stoch_rsi_d", "timeframe", "last_updated_at"
    ]

    filtered = [{k: v for k, v in row.items() if k in allowed_columns} for row in data]

    try:
        sb.table("signal_market_structure_core_raw").upsert(
            filtered, on_conflict=["symbol", "timeframe", "signal_time"]
        ).execute()
        print(f"[ok] Upserted {len(filtered)} rows.")
    except Exception as e:
        print(f"[error] Upsert failed: {e}")

# ========= REFRESH MATERIALIZED VIEW =========
def refresh_view():
    try:
        sb.rpc("exec_sql", {"sql": f"REFRESH MATERIALIZED VIEW CONCURRENTLY {REFRESH_VIEW};"}).execute()
        print(f"[refresh] Materialized view {REFRESH_VIEW} refreshed.")
    except Exception as e:
        print(f"[warn] View refresh failed: {e}")

# ========= MAIN LOOP =========
def main():
    print(f"[start] Market structure bucket ingest at {datetime.now().isoformat()}")
    total = 0
    for tf in TIMEFRAMES:
        data = fetch_combined_data(tf)
        upsert_signal_data(data)
        total += len(data)
        time.sleep(3)
    refresh_view()
    print(f"[done] {total} total rows processed across {TIMEFRAMES}.")

if __name__ == "__main__":
    main()
