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
BATCH_HOURS = 6       # total window (past 6 hours)
CHUNK_HOURS = 1       # process 1 hour at a time
TABLE_NAME = "signal_market_structure_agg_tf"
TIMEFRAMES = ["15m", "1h", "1d"]

# ========= HELPERS =========
def add_buckets(ts):
    if isinstance(ts, str):
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00")) if "Z" in ts else datetime.fromisoformat(ts)
    else:
        dt = ts
    b15 = dt.replace(minute=(dt.minute // 15) * 15, second=0, microsecond=0)
    b1h = dt.replace(minute=0, second=0, microsecond=0)
    b1d = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return b15, b1h, b1d

# ========= QUERY BUILDER =========
def build_query(tf, start_ts, end_ts):
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
        left join v_vwap_24h v on v.symbol = d.symbol
        left join v_funding_rates f on f.symbol = d.symbol
        left join v_open_interest o on o.symbol = d.symbol
        left join binance_ohlcv p on p.symbol = d.symbol and p.interval = '1h'
        left join v_ai_signal_rsi r on r.symbol = d.symbol and r.timeframe = '1d'
        where d.signal_time between '{start_ts}' and '{end_ts}'
        order by d.signal_time desc
        limit {LIMIT_ROWS}
    )
    select * from core;
    """

# ========= FETCH + UPSERT =========
def fetch_and_upsert(tf, start_ts, end_ts):
    q = build_query(tf, start_ts, end_ts)
    try:
        res = sb.rpc("exec_sql", {"sql": q}).execute()
        data = res.data or []
        if not data:
            print(f"[skip:{tf}] No rows between {start_ts} → {end_ts}")
            return 0

        now_ts = datetime.now(timezone.utc).isoformat()
        for r in data:
            r["last_updated_at"] = now_ts
            b15, b1h, b1d = add_buckets(r["signal_time"])
            r["bucket_15m"] = b15.isoformat()
            r["bucket_1h"] = b1h.isoformat()
            r["bucket_1d"] = b1d.isoformat()
            r["timeframe"] = tf

        allowed = [
            "symbol", "signal_time", "delta_strength", "delta_direction",
            "cvd_strength", "cvd_direction", "vwap_deviation", "funding_rate",
            "open_interest", "price_close", "trade_volume",
            "rsi_14", "stoch_rsi_k", "stoch_rsi_d",
            "bucket_15m", "bucket_1h", "bucket_1d",
            "timeframe", "last_updated_at"
        ]
        filtered = [{k: v for k, v in r.items() if k in allowed} for r in data]

        sb.table("signal_market_structure_agg_tf").upsert(
            filtered, on_conflict=["symbol", "timeframe", "signal_time"]
        ).execute()
        print(f"[ok:{tf}] Upserted {len(filtered)} rows ({start_ts} → {end_ts})")
        return len(filtered)

    except Exception as e:
        print(f"[error:{tf}] {e}")
        return 0

# ========= MAIN LOOP =========
def main():
    print(f"[start] Market structure TF aggregation at {datetime.now(timezone.utc).isoformat()}")

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=BATCH_HOURS)

    current = start_time
    while current < end_time:
        chunk_end = min(current + timedelta(hours=CHUNK_HOURS), end_time)
        for tf in TIMEFRAMES:
            rows = fetch_and_upsert(tf, current.isoformat(), chunk_end.isoformat())
            print(f"[chunk:{tf}] {rows} rows processed ({current} → {chunk_end})\n")
        current = chunk_end

    print("[complete] All timeframes updated.")

if __name__ == "__main__":
    main()




