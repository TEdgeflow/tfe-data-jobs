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
    "liquidation": 0.15,
    "volume": 0.10,
}

# ========= GET DATA =========
def get_latest_signal_inputs(symbol: str, timeframe: str = "5m"):
    """Fetch latest factor data from Supabase views for a given symbol + timeframe"""

    # VWAP
    vwap = sb.table("binance_vwap_agg") \
        .select("vwap, volume_quote, bucket_start") \
        .eq("symbol", symbol).eq("timeframe", timeframe) \
        .order("bucket_start", desc=True).limit(1).execute()

    # Delta
    delta = sb.table("v_signal_delta") \
        .select("strength_value, signal_time") \
        .eq("symbol", symbol).eq("timeframe", timeframe) \
        .order("signal_time", desc=True).limit(1).execute()

    # CVD
    cvd = sb.table("v_signal_cvd") \
        .select("strength_value, signal_time") \
        .eq("symbol", symbol).eq("timeframe", timeframe) \
        .order("signal_time", desc=True).limit(1).execute()

    # Orderbook agg
    ob = sb.table("binance_orderbook_agg_5m") \
        .select("bid_vol10, ask_vol10, bucket_5m") \
        .eq("symbol", symbol) \
        .order("bucket_5m", desc=True).limit(1).execute()

    # Liquidations
    liq = sb.table("v_liquidation_agg") \
        .select("long_liquidations, short_liquidations, last_update") \
        .eq("symbol", symbol) \
        .order("last_update", desc=True).limit(1).execute()

    # Trades agg (volume source)
    trades = sb.table("binance_trades_agg_5m") \
        .select("bucket_5m, buy_vol, sell_vol, delta, cvd") \
        .eq("symbol", symbol) \
        .order("bucket_5m", desc=True).limit(1).execute()

    # ========= Factor Scores =========
    vwap_score = 1 if vwap.data and vwap.data[0]["vwap"] > 0 else 0
    delta_score = 1 if delta.data and delta.data[0]["strength_value"] > 0 else 0
    cvd_score = 1 if cvd.data and cvd.data[0]["strength_value"] > 0 else 0
    orderbook_score = 1 if ob.data and ob.data[0]["bid_vol10"] > ob.data[0]["ask_vol10"] else 0
    liquidation_score = 1 if liq.data and liq.data[0]["long_liquidations"] > liq.data[0]["short_liquidations"] else 0
    volume_score = 1 if trades.data and (trades.data[0]["buy_vol"] + trades.data[0]["sell_vol"]) > 1_000_000 else 0
    
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "vwap_score": vwap_score,
        "delta_score": delta_score,
        "cvd_score": cvd_score,
        "orderbook_score": orderbook_score,
        "liquidation_score": liquidation_score,
        "volume_score": volume_score,
    }, trades

# ========= SCORING =========
def calculate_confidence(scores):
    total = sum(
        scores[f"{f}_score"] * WEIGHTS[f]
        for f in WEIGHTS
    )
    return round(total * 100, 2)

def get_direction(scores):
    positive = scores["vwap_score"] + scores["delta_score"] + scores["cvd_score"] + scores["orderbook_score"]
    negative = scores["liquidation_score"] + scores["volume_score"]
    return "LONG" if positive >= negative else "SHORT"

# ========= INSERT =========
def insert_signal(symbol, timeframe, scores, trades):
    # use bucket_xx time as the signal_time
    signal_time = None
    if trades.data and "bucket_5m" in trades.data[0]:
        signal_time = trades.data[0]["bucket_5m"]
    elif trades.data and "bucket_15m" in trades.data[0]:
        signal_time = trades.data[0]["bucket_15m"]
    elif trades.data and "bucket_1h" in trades.data[0]:
        signal_time = trades.data[0]["bucket_1h"]

    row = {
        "symbol": symbol,
        "timeframe": timeframe,
        "signal_time": signal_time,
        "vwap_score": scores.get("vwap_score", 0),
        "delta_score": scores.get("delta_score", 0),
        "cvd_score": scores.get("cvd_score", 0),
        "orderbook_score": scores.get("orderbook_score", 0),
        "liquidation_score": scores.get("liquidation_score", 0),
        "volume_score": scores.get("volume_score", 0),
        "direction": scores.get("direction", "NEUTRAL")
    }

    res = sb.table("ai_signals_shortterm").insert(row).execute()
    return res


# ========= MAIN =========
if __name__ == "__main__":
    symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]  # extend later
    for s in symbols:
        scores, trades = get_latest_signal_inputs(s, timeframe="5m")
        scores["final_score"] = calculate_confidence(scores)
        scores["direction"] = get_direction(scores)
        insert_signal(s, "5m", scores, trades)



