import os
from datetime import datetime, timezone
from supabase import create_client

# === Supabase connection ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
sb = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_daybias_inputs(symbol: str, timeframe: str = "1h"):
    """Pull long-term trend/bias data for signal scoring."""

    # === VWAP ===
    vwap = sb.table("binance_vwap_agg") \
        .select("*") \
        .eq("symbol", symbol) \
        .eq("timeframe", timeframe) \
        .order("bucket_start", desc=True) \
        .limit(1) \
        .execute()
    vwap_score = 1 if vwap.data and vwap.data[0]["vwap"] > 1 else -1 if vwap.data else 0

    # === Delta ===
    delta = sb.table("v_signal_delta") \
        .select("*") \
        .eq("symbol", symbol) \
        .eq("timeframe", timeframe) \
        .order("signal_time", desc=True) \
        .limit(1) \
        .execute()
    delta_score = 1 if delta.data and delta.data[0]["strength_value"] > 0 else -1 if delta.data else 0

    # === Whale Inflow ===
    whale = sb.table("nansen_whaleflows") \
        .select("*") \
        .eq("token", symbol) \
        .order("ts", desc=True) \
        .limit(1) \
        .execute()
    whale_score = 1 if whale.data and whale.data[0]["inflow_usd"] > 0 else 0

    # === Unlocks ===
    unlocks = sb.table("droptabs_unlocks") \
        .select("*") \
        .eq("symbol", symbol) \
        .order("unlock_date", desc=True) \
        .limit(1) \
        .execute()
    unlock_score = -1 if unlocks.data else 0

    # Final score
    total_score = vwap_score + delta_score + whale_score + unlock_score
    bias = "BULLISH" if total_score > 0 else "BEARISH" if total_score < 0 else "NEUTRAL"

    return {
        "symbol": symbol,
        "vwap_score": vwap_score,
        "delta_score": delta_score,
        "whale_score": whale_score,
        "unlock_score": unlock_score,
        "bias": bias
    }

def insert_daybias_signal(symbol, timeframe, scores):
    """Insert results into ai_signals_daybias"""
    row = {
        "symbol": symbol,
        "signal_date": datetime.now(timezone.utc).date().isoformat(),
        "vwap_trend_score": scores["vwap_score"],
        "delta_trend_score": scores["delta_score"],
        "whale_inflow_score": scores["whale_score"],
        "unlock_bias_score": scores["unlock_score"],
        "confidence_score": (scores["vwap_score"] + scores["delta_score"] + scores["whale_score"] + scores["unlock_score"]) * 25,
        "bias": scores["bias"],
        "created_at": datetime.now(timezone.utc).isoformat()
    }

    res = sb.table("ai_signals_daybias").insert(row).execute()
    print(f"[daybias] {symbol} {timeframe} {scores['bias']} (confidence={row['confidence_score']}%)")
    return res

if __name__ == "__main__":
    symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    for s in symbols:
        scores = get_daybias_inputs(s, timeframe="1h")
        insert_daybias_signal(s, "1h", scores)


