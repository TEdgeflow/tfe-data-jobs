import os, time, math, requests
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from supabase import create_client

# ========= ENV =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SYMBOLS = [s.strip().upper() for s in os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT").split(",") if s.strip()]
INTERVALS = [s.strip() for s in os.getenv("INTERVALS", "1h,4h,1d").split(",") if s.strip()]
CANDLE_LIMIT = int(os.getenv("CANDLE_LIMIT", "1000"))  # fetch enough history for RSI/Stoch RSI
BINANCE_BASE = "https://api.binance.com"

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing Supabase credentials")

sb = create_client(SUPABASE_URL, SUPABASE_KEY)

# ========= Helpers =========
def fetch_klines(symbol: str, interval: str, limit: int = 1000) -> pd.DataFrame:
    ivl_map = {"1h": "1h", "4h": "4h", "1d": "1d"}
    if interval not in ivl_map:
        raise ValueError(f"Unsupported interval: {interval}")

    params = {"symbol": symbol, "interval": ivl_map[interval], "limit": limit}
    r = requests.get(f"{BINANCE_BASE}/api/v3/klines", params=params, timeout=30)
    r.raise_for_status()
    raw = r.json()

    rows = []
    for k in raw:
        # kline schema: [openTime, open, high, low, close, volume, closeTime, ...]
        ts = datetime.utcfromtimestamp(k[0] / 1000.0).replace(tzinfo=timezone.utc)
        rows.append({
            "symbol": symbol,
            "interval": interval,
            "ts": ts.isoformat(),
            "open": float(k[1]),
            "high": float(k[2]),
            "low":  float(k[3]),
            "close": float(k[4]),
            "volume": float(k[5]),
        })
    return pd.DataFrame(rows)

def upsert(table: str, records: list, on_conflict: str):
    if not records:
        return
    # supabase-py upsert supports on_conflict as comma-separated string
    sb.table(table).upsert(records, on_conflict=on_conflict).execute()

def rsi(series: pd.Series, length: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.ewm(alpha=1/length, min_periods=length, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/length, min_periods=length, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    rsi_vals = 100 - (100 / (1 + rs))
    return rsi_vals.fillna(method="bfill")

def stoch_rsi(rsi_series: pd.Series, length: int = 14, smooth_k: int = 3, smooth_d: int = 3):
    rsi_min = rsi_series.rolling(length).min()
    rsi_max = rsi_series.rolling(length).max()
    denom = (rsi_max - rsi_min).replace(0.0, np.nan)
    stoch = ((rsi_series - rsi_min) / denom).clip(0, 1) * 100.0
    k = stoch.rolling(smooth_k).mean()
    d = k.rolling(smooth_d).mean()
    return k.fillna(method="bfill"), d.fillna(method="bfill")

def compute_and_upsert_indicators(df: pd.DataFrame):
    if df.empty: 
        return
    df = df.sort_values("ts")
    rsi14 = rsi(df["close"], 14)
    k, d = stoch_rsi(rsi14, 14, 3, 3)

    out = []
    for i, row in df.iterrows():
        out.append({
            "symbol": row["symbol"],
            "interval": row["interval"],
            "ts": row["ts"],
            "rsi_14": float(rsi14.loc[i]) if not math.isnan(rsi14.loc[i]) else None,
            "stoch_rsi_k_14_14_3": float(k.loc[i]) if not math.isnan(k.loc[i]) else None,
            "stoch_rsi_d_14_14_3": float(d.loc[i]) if not math.isnan(d.loc[i]) else None,
        })
    upsert("technical_indicators", out, on_conflict="symbol,interval,ts")

def main():
    print("[job] starting OHLCV + RSI ingestion")
    for symbol in SYMBOLS:
        for interval in INTERVALS:
            try:
                df = fetch_klines(symbol, interval, limit=CANDLE_LIMIT)
                # Upsert OHLCV
                upsert("binance_ohlcv", df.to_dict("records"), on_conflict="symbol,interval,ts")
                # Compute + upsert indicators
                compute_and_upsert_indicators(df)
                print(f"[ok] {symbol} {interval}: {len(df)} candles")
                time.sleep(0.2)  # be gentle with API
            except Exception as e:
                print(f"[warn] {symbol} {interval}: {e}")
                time.sleep(1.0)
    print("[job] done")

if __name__ == "__main__":
    main()
