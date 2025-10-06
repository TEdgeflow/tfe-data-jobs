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
LIMIT_ROWS = int(os.getenv("LIMIT_ROWS", "1000"))
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "3"))
RETRY_LIMIT = 3

# ========= FETCH HISTORICAL DATA =========
def fetch_historical_data():
    try:
        existing = sb.table("signal_market_structure_core_raw") \
            .select("symbol, max(signal_time)", count="exact") \
            .group("symbol") \
            .execute()
        latest_map = {row["symbol"]: row["max"] for row in existing.data} if existing.data else {}
        if latest_map:
            conditions = [
                f"(d.symbol = '{symbol}' and d.signal_time > '{last_time}')"
                for symbol, last_time in latest_map.items() if last_time
            ]
            where_clause = " OR ".join(conditions)
            print(f"[info] Incremental fetch for {len(latest_map)} symbols.")
        else:
            where_clause = f"d.signal_time > now() - interval '{LOOKBACK_DAYS} days'"
            print(f"[warn] No existing data found. Full lookback {LOOKBACK_DAYS} days.")

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
          where {where_clause}
          order by d.signal_time desc
          limit {LIMIT_ROWS}
        )
        select * from core;
        """

        res = sb.rpc("exec_sql", {"sql": query}).execute()
        if not res.data:
            print("[skip] No new records found.")
            return []
        print(f"[fetch] Retrieved {len(res.data)} backfill records.")
        return res.data
    except Exception as e:
        print(f"[error] Failed fetching historical data: {e}")
        return []

# ========= UPSERT =========
def upsert_historical_data(data):
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
            print(f"[ok] Backfilled {len(filtered_data)} rows successfully.")
            return
        except Exception as e:
            print(f"[warn] Attempt {attempt+1} failed: {e}")
            time.sleep(3)
    print("[error] All upsert attempts failed.")

# ========= REFRESH MATERIALIZED VIEW =========
def refresh_materialized_view():
    try:
        print("[refresh] Refreshing materialized view v_signal_market_structure_core_mat...")
        sb.rpc("exec_sql", {"sql": "REFRESH MATERIALIZED VIEW CONCURRENTLY v_signal_market_structure_core_mat;"}).execute()
        print("[ok] Materialized view refreshed successfully.")
    except Exception as e:
        print(f"[warn] Failed to refresh materialized view: {e}")

# ========= MAIN =========
def main():
    print(f"[start] Backfill job running at {datetime.now().isoformat()}")
    data = fetch_historical_data()
    upsert_historical_data(data)
    refresh_materialized_view()
    print(f"[done] Processed {len(data)} total records.")

if __name__ == "__main__":
    main()
