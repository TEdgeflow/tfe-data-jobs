import os
from supabase import create_client
from datetime import datetime, timezone

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing Supabase credentials")

sb = create_client(SUPABASE_URL, SUPABASE_KEY)

# ========= WEIGHTS =========
WEIGHTS = {
    "vwap_trend": 0.20,
    "delta_trend": 0.20,
    "cvd_alignment": 0.15,
    "orderbook": 0.15,
    "liquidation_bias": 0.10,
    "unlock_bias": 0.10,
    "whale_inflow": 0.05,
    "volume_rsi": 0.05,
}

# ========= GET DATA =========
def get_daybias_inputs(symbol: str, timeframe: str = "1h"):
    """
    Fetch latest factor data from Supabase views for a given symbol + timeframe
    Adjusted to match your table schema (no bucket_start / timestamp mismatch).
    """

    # ---- Replace with your real views ----
    vwap = sb.table("binance_vwap_agg").select("*").eq("symbol", symbol).eq("timeframe", timeframe).order("bucket", desc=True).limit(1).execute()
    delta = sb.table("v_signal_delta").select("*").eq("symbol", symbol).eq("timeframe", timeframe).order("signal_time", desc=True).limit(1).execute()
    cvd = sb.table("v_signal_cvd").select("*").eq("symbol", symbol).eq("timeframe", timeframe).order("signal_time", desc=True).limit(1).execute()
    ob = sb.table("binance_orderbook_agg_1h").select("*").eq("symbol", symbol).order("bucket_1h", desc=True).limit(1).execute()
    liq = sb.table("binance_liquidations").select("*").eq("symbol", symbol).order("time", desc=True).limit(20).execute()
    unlock = sb.table("droptabs_unlocks").select("*").eq("coin_symbol", symbol).order("unlock_date", desc=True).limit(1).execute()
    inflow = sb.table("nansen_whaleflows").select("*").eq("symbol", symbol).order("timestamp", desc=True).limit(1).execute()
    volume = sb.table("market_data").select("*").eq("symbol", symbol).order("timestamp", desc=True).limit(1).execute()

    # ---- Factor scoring ----
    vwap_score = 1 if vwap.data and vwap.data[0].get("trend") == "bullish" else 0
    delta_score = 1 if delta.data and delta.data[0].get("strength_value", 0) > 0 else 0
    cvd_score = 1 if cvd.data and cvd.data[0].get("cvd", 0) > 0 else 0
    orderbook_score = 1 if ob.data and ob.data[0].get("bid_vol", 0) > ob.data[0].get("ask_vol", 0) else 0
    liquidation_score = 1 if liq.data and len(liq.data) > 10 else 0
    unlock_score = 1 if unlock.data and unlock.data[0].get("days_until_unlock", 999) < 30 else 0
    inflow_score = 1 if inflow.data and inflow.data[0].get("inflow_usd", 0) > 1000000 else 0
    volume_score = 1 if volume.data and volume.data[0].get("volume_usd", 0) > 5000000 else 0

    return {
        "symbol": symbol,
        "signal_date": datetime.now(timezone.utc).date().isoformat(),
        "vwap_trend_score": vwap_score,
        "delta_trend_score": delta_score,
        "cvd_alignment_score": cvd_score,
        "orderbook_score": orderbook_score,
        "liquidation_bias_score": liquidation_score,
        "unlock_bias_score": unlock_score,
        "whale_inflow_score": inflow_score,
        "volume_rsi_score": volume_score,
    }

# ========= SCORING =========
def calculate_confidence(scores):
    total = sum(
        scores[f"{factor}_score"] * WEIGHTS[factor]
        for factor in ["vwap_trend","delta_trend","cvd_alignment","orderbook","liquidation_bias","unlock_bias","whale_inflow","volume_rsi"]
    )
    return round(total * 100, 2)

def get_bias(scores):
    bullish = (
        scores["vwap_trend_score"] +
        scores["delta_trend_score"] +
        scores["cvd_alignment_score"] +
        scores["orderbook_score"]
    )
    bearish = 8 - bullish
    return "BULLISH" if bullish >= bearish else "BEARISH"

# ========= INSERT =========
def insert_daybias(symbol, timeframe, scores):
    confidence = calculate_confidence(scores)
    bias = get_bias(scores)

    row = {
        "symbol": symbol,
        "signal_date": scores["signal_date"],
        "vwap_trend_score": scores["vwap_trend_score"],
        "delta_trend_score": scores["delta_trend_score"],
        "cvd_alignment_score": scores["cvd_alignment_score"],
        "orderbook_score": scores["orderbook_score"],
        "liquidation_bias_score": scores["liquidation_bias_score"],
        "unlock_bias_score": scores["unlock_bias_score"],
        "whale_inflow_score": scores["whale_inflow_score"],
        "volume_rsi_score": scores["volume_rsi_score"],
        "confidence_score": confidence,
        "bias": bias,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    res = sb.table("ai_signals_daybias").insert(row).execute()
    print(f"[daybias_signal] {symbol} {timeframe} {bias} {confidence}%")
    return res

# ========= MAIN =========
if __name__ == "__main__":
    symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    for s in symbols:
        scores = get_daybias_inputs(s, timeframe="1h")
        insert_daybias(s, "1h", scores)

