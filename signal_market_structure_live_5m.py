import os
from datetime import datetime, timezone, timedelta
from supabase import create_client

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing Supabase credentials")

sb = create_client(SUPABASE_URL, SUPABASE_KEY)

# ========= CONFIG =========
LIMIT_ROWS = 2000
TIME_WINDOW_HOURS = 12  # how far back to fetch
TABLE_NAME = "signal_market_structure_agg_5m"

# ========= HELPERS =========
def add_buckets(ts):
    if isinstance(ts, str):
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00")) if "Z" in ts else datetime.fromisoformat(ts)
    else:
        dt = ts
    return dt.replace(minute=(dt.minute // 5) * 5, second=0, microsecond=0)

# ========= QUERY =========
def build_query():
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

# ========= MAIN LOOP =========
def main():
    print(f"[start] Market structure 5m aggregation at {datetime.now(timezone.utc).isoformat()}")
    q = build_query()
    try:
        res = sb.rpc("exec_sql", {"sql": q}).execute()
        data = res.data or []
        if not data:
            print("[skip] No data returned.")
            return

        now_ts = datetime.now(timezone.utc).isoformat()
        for r in data:
            r["last_updated_at"] = now_ts
            bucket_5m = add_buckets(r["signal_time"])
            r["bucket_5m"] = bucket_5m.isoformat()

        allowed = [
            "symbol", "signal_time", "delta_strength", "delta_direction",
            "cvd_strength", "cvd_direction", "vwap_deviation", "funding_rate",
            "open_interest", "price_close", "trade_volume",
            "rsi_14", "stoch_rsi_k", "stoch_rsi_d", "bucket_5m", "last_updated_at"
        ]
        filtered = [{k: v for k, v in r.items() if k in allowed} for r in data]

        sb.table("signal_market_structure_agg_5m_unique").upsert(
            filtered, on_conflict=["symbol", "timeframe", "signal_time"]
        ).execute()
        print(f"[ok] Upserted {len(filtered)} rows to {TABLE_NAME}")

    except Exception as e:
        print(f"[error] 5m agg: {e}")

if __name__ == "__main__":
    main()

