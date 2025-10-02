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
    "vwap": 0.25,
    "delta": 0.20,
    "cvd": 0.20,
    "orderbook": 0.15,
    "liquidation": 0.10,
    "volume": 0.10,
}

# ========= GET DATA =========
def get_latest_signal_inputs(symbol: str, timeframe: str = "5m"):
    """Fetch latest factor data from Supabase views for a given symbol + timeframe"""

    vwap = sb.table("binance_vwap_agg").select("*").eq("symbol", symbol).eq("timeframe", timeframe).order("bucket_start", desc=True).limit(1).execute()
    delta = sb.table("v_signal_delta").select("*").eq("symbol", symbol).eq("timeframe", timeframe).order("signal_time", desc=True).limit(1).execute()
    cvd = sb.table("v_signal_cvd").select("*").eq("symbol", symbol).eq("timeframe", timeframe).order("signal_time", desc=True).limit(1).execute()
    ob = sb.table("binance_orderbook_agg_5m").select("*").eq("symbol", symbol).order("bucket_5m", desc=True).limit(1).execute()
    liq = sb.table("binance_liquidations").select("*").eq("symbol", symbol).order("time", desc=True).limit(10).execute()

    # ===== DEBUG PRINTS =====
    print("DEBUG vwap:", vwap.data)
    print("DEBUG delta:", delta.data)
    print("DEBUG cvd:", cvd.data)
    print("DEBUG orderbook:", ob.data)
    print("DEBUG liquidation:", liq.data)

    # Mock factor scoring (replace with actual metrics)
    vwap_score = 1 if vwap.data and "vwap" in vwap.data[0] and vwap.data[0]["vwap"] < vwap.data[0].get("close_price", 999999) else 0
    delta_score = 1 if delta.data and "strength_value" in delta.data[0] and delta.data[0]["strength_value"] > 0 else 0
    cvd_score = 1 if cvd.data and "strength_value" in cvd.data[0] and cvd.data[0]["strength_value"] > 0 else 0
   orderbook_score = (
    1 if ob.data and ob.data[0]["bid_vol10"] > ob.data[0]["ask_vol10"] else 0
)

    liquidation_score = 1 if liq.data and len(liq.data) > 3 else 0
    volume_score = 1 if vwap.data and "volume_quote" in vwap.data[0] and vwap.data[0]["volume_quote"] > 1000000 else 0

    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "vwap_score": vwap_score,
        "delta_score": delta_score,
        "cvd_score": cvd_score,
        "orderbook_score": orderbook_score,
        "liquidation_score": liquidation_score,
        "volume_score": volume_score,
    }

# ========= SCORING =========
def calculate_confidence(scores):
    total = sum(
        scores[f] * WEIGHTS[f]
        for f in ["vwap", "delta", "cvd", "orderbook", "liquidation", "volume"]
    )
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
        "confidence_score": confidence,
        "direction": direction,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    res = sb.table("ai_signals_shortterm").insert(row).execute()
    print(f"[shortterm_signal] {symbol} {timeframe} {direction} {confidence}%")
    return res

# ========= MAIN =========
if __name__ == "__main__":
    symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]  # extend to your list
    for s in symbols:
        scores = get_latest_signal_inputs(s, timeframe="5m")
        insert_signal(s, "5m", scores)
