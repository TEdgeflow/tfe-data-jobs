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
    "volume": 0.10,
    "whale_inflow": 0.05,
    "unlock_risk": 0.10,
}

# ========= GET DATA =========
def get_daybias_inputs(symbol: str, timeframe: str = "1h"):
    vwap = sb.table("binance_vwap_agg").select("*") \
        .eq("symbol", symbol).eq("timeframe", timeframe) \
        .order("bucket_start", desc=True).limit(1).execute()

    delta = sb.table("v_signal_delta").select("*") \
        .eq("symbol", symbol).eq("timeframe", timeframe) \
        .order("bucket_start", desc=True).limit(1).execute()

    cvd = sb.table("v_signal_cvd").select("*") \
        .eq("symbol", symbol).eq("timeframe", timeframe) \
        .order("bucket_start", desc=True).limit(1).execute()

    ob = sb.table("binance_orderbook_agg_1h").select("*") \
        .eq("symbol", symbol).order("bucket_1h", desc=True).limit(1).execute()

    liq = sb.table("binance_liquidations").select("*") \
        .eq("symbol", symbol).order("time", desc=True).limit(20).execute()

    inflow = sb.table("nansen_whaleflows").select("*") \
        .eq("symbol", symbol).order("timestamp", desc=True).limit(1).execute()

    unlock = sb.table("v_unlock_risk_proxy").select("*") \
        .eq("symbol", symbol).limit(1).execute()

    # Scores
    vwap_score = 1 if vwap.data and vwap.data[0]["vwap"] < vwap.data[0]["close_price"] else 0
    delta_score = 1 if delta.data and delta.data[0]["net_delta"] > 0 else 0
    cvd_score = 1 if cvd.data and cvd.data[0]["cvd"] > 0 else 0
    orderbook_score = 1 if ob.data and ob.data[0]["bid_vol"] > ob.data[0]["ask_vol"] else 0
    liquidation_score = 1 if liq.data and len(liq.data) > 5 else 0
    volume_score = 1 if vwap.data and vwap.data[0]["volume_quote"] > 5000000 else 0
    inflow_score = 1 if inflow.data and inflow.data[0]["inflow_usd"] > 100000 else 0
    unlock_score = 1 if unlock.data and "High" in unlock.data[0]["risk_level"] else 0

    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "vwap_score": vwap_score,
        "delta_score": delta_score,
        "cvd_score": cvd_score,
        "orderbook_score": orderbook_score,
        "liquidation_score": liquidation_score,
        "volume_score": volume_score,
        "whale_inflow_score": inflow_score,
        "unlock_risk_score": unlock_score,
    }

# ========= SCORING =========
def calculate_confidence(scores):
    total = sum(scores[f] * WEIGHTS[f] for f in WEIGHTS)
    return round(total * 100, 2)

def get_direction(scores):
    bullish = scores["vwap_score"] + scores["delta_score"] + scores["cvd_score"] + scores["orderbook_score"]
    bearish = 6 - bullish
    return "LONG" if bullish >= bearish else "SHORT"

# ========= INSERT =========
def insert_signal(symbol, timeframe, scores):
    confidence = calculate_confidence(scores)
    direction = get_direction(scores)

    row = {
        "symbol": symbol,
        "timeframe": timeframe,
        "vwap_score": scores["vwap_score"],
        "delta_score": scores["delta_score"],
        "cvd_score": scores["cvd_score"],
        "orderbook_score": scores["orderbook_score"],
        "liquidation_score": scores["liquidation_score"],
        "volume_score": scores["volume_score"],
        "whale_inflow_score": scores["whale_inflow_score"],
        "unlock_risk_score": scores["unlock_risk_score"],
        "confidence_score": confidence,
        "direction": direction,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    sb.table("ai_signals_daybias").insert(row).execute()
    print(f"[daybias_signal] {symbol} {timeframe} {direction} {confidence}%")

# ========= MAIN =========
if __name__ == "__main__":
    symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    for s in symbols:
        scores = get_daybias_inputs(s, timeframe="1h")
        insert_signal(s, "1h", scores)



