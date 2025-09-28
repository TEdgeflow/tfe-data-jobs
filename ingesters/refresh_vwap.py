# /ingesters/refresh_vwap.py
import os
import pandas as pd
from datetime import datetime, timedelta, timezone
from supabase import create_client

# ===== ENV =====
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")  # service role, not anon
LOOKBACK_DAYS = int(os.getenv("VWAP_LOOKBACK_DAYS", "7"))  # adjust as needed

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")

sb = create_client(SUPABASE_URL, SUPABASE_KEY)

# ===== 1. Pull trades =====
def fetch_trades(symbol: str, lookback_days: int = 7):
    since = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).isoformat()
    res = sb.table("binance_trades").select("symbol, ts, price, qty") \
             .eq("symbol", symbol).gte("ts", since).execute()
    df = pd.DataFrame(res.data)
    if df.empty:
        return pd.DataFrame()
    df["ts"] = pd.to_datetime(df["ts"])
    df["notional"] = df["price"].astype(float) * df["qty"].astype(float)
    return df

# ===== 2. Aggregate VWAP =====
def compute_vwap(df: pd.DataFrame, timeframe: str):
    if df.empty:
        return pd.DataFrame()

    rule_map = {"5m": "5min", "15m": "15min", "1h": "1H", "1d": "1D", "1w": "1W"}
    rule = rule_map[timeframe]

    agg = df.resample(rule, on="ts").apply({
        "price": "last",
        "qty": "sum",
        "notional": "sum"
    }).dropna().reset_index()

    agg["vwap"] = agg["notional"] / agg["qty"]
    agg["bucket_start"] = agg["ts"]
    agg["bucket_end"] = agg["ts"] + pd.to_timedelta(rule)

    return agg[["bucket_start", "bucket_end", "vwap", "price", "notional", "qty"]]

# ===== 3. Upsert to Supabase =====
def upsert_vwap(symbol: str, tf: str, df: pd.DataFrame):
    if df.empty:
        return
    rows = []
    for _, r in df.iterrows():
        rows.append({
            "symbol": symbol,
            "timeframe": tf,
            "bucket_start": r["bucket_start"].isoformat(),
            "bucket_end": r["bucket_end"].isoformat(),
            "vwap": round(float(r["vwap"]), 8),
            "close_price": round(float(r["price"]), 8),
            "volume_quote": round(float(r["notional"]), 8),
            "volume_base": round(float(r["qty"]), 8),
        })
    sb.table("binance_vwap_agg").upsert(rows, on_conflict="symbol,timeframe,bucket_start").execute()

# ===== 4. Main run =====
def run(symbols: list[str]):
    for sym in symbols:
        print(f"[VWAP] Processing {sym}")
        trades = fetch_trades(sym, LOOKBACK_DAYS)
        if trades.empty:
            print(f"[VWAP] No trades for {sym}")
            continue
        for tf in ["5m", "15m", "1h", "1d", "1w"]:
            df_vwap = compute_vwap(trades, tf)
            upsert_vwap(sym, tf, df_vwap)

if __name__ == "__main__":
    symbols = os.getenv("VWAP_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT").split(",")
    run([s.strip().upper() for s in symbols if s.strip()])
