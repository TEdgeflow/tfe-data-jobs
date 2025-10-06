import os
from datetime import datetime, timezone, timedelta
from supabase import create_client, Client

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing Supabase credentials")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ========= CONFIG =========
TIME_WINDOW_HOURS = 12   # pull data from last 12 hours
LIMIT_ROWS = 500         # fetch limit per run

# ========= MAIN INGESTION QUERY =========
def fetch_combined_data():
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
      left join v_ai_signals_rsi r on r.symbol = d.symbol and r.timeframe = '1d'
      where d.signal_time > now() - interval '{TIME_WINDOW_HOURS} hours'
      limit {LIMIT_ROWS}
    )
    select * from core;
    """

    res = sb.table("v_signal_market_structure_core_raw").select("*").limit(500).execute()
    return res.data if res.data else []

# Filter columns to match actual table schema
allowed_columns = [
    "symbol", "signal_time", "delta_strength", "delta_direction",
    "cvd_strength", "cvd_direction", "vwap_deviation",
    "funding_rate", "open_interest", "price_close",
    "trade_volume", "rsi_14", "stoch_rsi_k", "stoch_rsi_d", "last_updated_at"
]
data = [{k: v for k, v in row.items() if k in allowed_columns} for row in data]


# ========= UPSERT DATA =========
def upsert_signal_data(data):
    if not data:
        print("[skip] No new data to upsert.")
        return

    for row in data:
        row["last_updated_at"] = datetime.now(timezone.utc).isoformat()

    try:
        sb.table("signal_market_structure_core_raw").upsert(
            data, on_conflict=["symbol", "signal_time"]
        ).execute()
        print(f"[ok] Upserted {len(data)} rows.")
    except Exception as e:
        print(f"[error] Upsert failed: {e}")

# ========= MAIN LOOP =========
def main():
    print(f"[start] Market structure ingest at {datetime.now().isoformat()}")
    data = fetch_combined_data()
    upsert_signal_data(data)
    print(f"[done] {len(data)} records processed.")

if __name__ == "__main__":
    main()
