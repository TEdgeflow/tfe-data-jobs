import os, time, math, requests
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from supabase import create_client

# ========= CONFIG =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Use Binance **Futures** (USDT-M) for perp contracts
BINANCE_FAPI = "https://fapi.binance.com"

# INTERVALS to compute
INTERVALS = [s.strip() for s in os.getenv("INTERVALS", "1h,4h,1d").split(",") if s.strip()]
CANDLE_LIMIT = int(os.getenv("CANDLE_LIMIT", "1000"))

# Optional filters:
#   - If SYMBOLS env is non-empty, we will use that exact list (comma-separated)
#   - Otherwise we auto-discover ALL USDT-M PERPETUAL symbols.
SYMBOLS_ENV = os.getenv("SYMBOLS", "").strip()

# Small pause between requests to be gentle on rate limits
SLEEP_S = float(os.getenv("SLEEP_S", "0.12"))

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing Supabase credentials")

sb = create_client(SUPABASE_URL, SUPABASE_KEY)

# ========= HELPERS =========
def log(msg): print(msg, flush=True)

def get_all_usdt_perp_symbols():
    """Fetch all USDT-M perpetual symbols that are TRADING."""
    url = f"{BINANCE_FAPI}/fapi/v1/exchangeInfo"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()
    symbols = []
    for s in data.get("symbols", []):
        if (
            s.get("quoteAsset") == "USDT" and
            s.get("contractType") == "PERPETUAL" and
            s.get("status") == "TRADING"
        ):
            symbols.append(s["symbol"])
    return sorted(symbols)

def discover_symbols():
    if SYMBOLS_ENV:
        syms = [x.strip().upper() for x in SYMBOLS_ENV.split(",") if x.strip()]
        log(f"[universe] using SYMBOLS from env ({len(syms)}): {', '.join(syms[:12])}{'...' if len(syms)>12 else ''}")
        return syms
    syms = get_all_usdt_perp_symbols()
    log(f"[universe] discovered ALL USDT-M PERP symbols: {len(syms)}")
    return syms

def fetch_klines(symbol: str, interval: str, limit: int) -> pd.DataFrame:
    """USDT-M Futures klines."""
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    r = requests.get(f"{BINANCE_FAPI}/fapi/v1/klines", params=params, timeout=30)
    r.raise_for_status()
    raw = r.json()
    rows = []
    for k in raw:
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
    if records:
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
    st = ((rsi_series - rsi_min) / denom).clip(0, 1) * 100.0
    k = st.rolling(smooth_k).mean()
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
    log("[job] start OHLCV + RSI for ALL USDT-M PERP")
    symbols = discover_symbols()
    for sym in symbols:
        for ivl in INTERVALS:
            try:
                df = fetch_klines(sym, ivl, limit=CANDLE_LIMIT)
                upsert("binance_ohlcv", df.to_dict("records"), on_conflict="symbol,interval,ts")
                compute_and_upsert_indicators(df)
                log(f"[ok] {sym} {ivl}: {len(df)} candles")
                time.sleep(SLEEP_S)  # rate-limit friendly
            except Exception as e:
                log(f"[warn] {sym} {ivl}: {e}")
                time.sleep(0.3)
    log("[job] done")

if __name__ == "__main__":
    main()

