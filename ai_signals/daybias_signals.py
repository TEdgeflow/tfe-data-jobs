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
    "vwap": 0.20,
    "delta": 0.20,
    "cvd": 0.20,
    "orderbook": 0.15,
    "liquidation": 0.10,
    "unlock": 0.05,
    "whale_inflow": 0.05,
    "volume": 0.05,
}

# ========= GET DATA =========
def get_daybias_inputs(symbol: str, timeframe: str = "1h"):
    """Fetch latest factor data for a given symbol + timeframe"""

    vwap = (
        sb.table("binance_vwap_agg")
        .select("*")
        .eq("symbol", symbol)
        .eq("timeframe", timeframe)
        .order("bucket_start", desc=True)   # FIXED
        .limit(1)
        .execute()
    )

    delta = (
        sb.table("v_signal_delta")
        .select("*")
        .eq("symbol", symbol)
        .eq("timeframe", timeframe)
        .order("signal_time", desc=True)
        .limit(1)
        .execute()
    )

    cvd = (
        sb.table("v_signal_cvd")
        .select("*")
        .eq("symbol", symbol)
        .eq("timeframe", timeframe)
        .order("signal_time", desc=True)
        .limit(1)
        .execute()
    )

    ob = (
        sb.table("binance_orderbook_agg_1h")
        .select("*")
        .eq("symbol", symbol)
        .order("bucket_1h", desc=True)
        .limit(1)
        .execute()
    )

    liq = (
        sb.table("binance_liquidations")
        .select("*")
        .eq("symbol", symbol)
        .order("time", desc=True)
        .limit(50)
        .execute()
    )

    inflow = (
        sb.table("nansen_whaleflows")
        .select("*")
        .eq("symbol", symbol)
        .order("time", desc=True)
        .limit(1)
        .execute()
    )

    unlock = (
        sb.table("droptabs_unlocks")
        .select("*")
        .eq("symbol", symbol)
        .order("unlock_date", desc=True)
        .limit(1)
        .execute()
    )

    # ---- Scoring logic (simple examples, expand as needed) ----
    vwap_score = 1 if vwap.data and vwap.data[0]["vwap"] < vwap.data[0]["volume_quote"] else 0
    delta_score = 1 if delta.data and delta.data[0]["strength_value"] > 0 else 0
    cvd_score = 1 if cvd.data and cvd.data[0]["cvd"] > 0 else 0
    orderbook_score = 1 if ob.data and ob.data[0]["bid_vol"] > ob.data[0]["ask_vol"] else 0
    liquidation_score = 1 if liq.data and len(liq.data) > 10 else 0
    inflow_score = 1 if inflow.data and inflow.data[0]["inflow_usd"] > 1000000 else 0
    unlock_score = 1 if unlock.data else 0
    volume_score = 1 if vwap.data and vwap.data[0]["volume_quote"] > 5000000 else 0

    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "vwap_score": vwap_score,
        "delta_score": delta_score,
        "cvd_score": cvd_score,
        "orderbook_score": orderbook_score,
        "liquidation_score": liquidation_score,
        "whale_inflow_score": inflow_score,
        "unlock_score": unlock_score,
        "volume_score": volume_score,
    }

# ========= SCORING =========
def calculate_confidence(scores):
    total = sum(
        scores[f"{k}_score"] * WEIGHTS[k]
        for k in WEIGHTS.keys()
    )
    return round(total * 100, 2)

def get_bias(scores):
    bullish = scores["vwap_score"] + scores["delta_score"] + scores["cvd_score"] + scores["orderbook_score"]
    bearish = 6 - bullish
    return "BULLISH" if bullish >= bearish else "BEARISH"

# ========= INSERT =========
def insert_daybias(symbol, timeframe, scores):
    confidence = calculate_confidence(scores)
    bias = get_bias(scores)

    row = {
        "symbol": symbol,
        "signal_date": datetime.now(timezone.utc).date().isoformat(),
        "vwap_trend_score": scores["vwap_score"],
        "delta_trend_score": scores["delta_score"],
        "cvd_alignment_score": scores["cvd_score"],
        "orderbook_score": scores["orderbook_score"],
        "liquidation_bias_score": scores["liquidation_score"],
        "unlock_bias_score": scores["unlock_score"],
        "whale_inflow_score": scores["whale_inflow_score"],
        "volume_rsi_score": scores["volume_score"],
        "confidence_score": confidence,
        "bias": bias,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    res = sb.table("ai_signals_daybias").insert(row).execute()
    print(f"[daybias_signal] {symbol} {timeframe} {bias} {confidence}%")
    return res

# ========= MAIN =========
if __name__ == "__main__":
    symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]  # extend list
    for s in symbols:
        scores = get_daybias_inputs(s, timeframe="1h")
        insert_daybias(s, "1h", scores)

