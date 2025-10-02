import os
from datetime import datetime, timezone
from supabase import create_client

# === Supabase connection ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
sb = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_latest_signal_inputs(symbol: str, timeframe: str = "5m"):
    """Pull short-term factors for signal scoring."""

    # === Orderbook Agg ===
    ob = sb.table("binance_orderbook_agg_5m") \
        .select("*") \
        .eq("symbol", symbol) \
        .order("bucket_5m", desc=True) \
        .limit(1) \
        .execute()

    if ob.data and len(ob.data) > 0:
        orderbook_score = 1 if ob.data[0]["bid_vol10"] > ob.data[0]["ask_vol10"] else -1
    else:
        orderbook_score = 0

    # === Liquidation Agg ===
    liq = sb.table("v_liquidation_agg") \
        .select("*") \
        .eq("symbol", symbol) \
        .eq("timeframe", timeframe) \
        .order("timestamp", desc=True) \
        .limit(1) \
        .execute()

    if liq.data and len(liq.data) > 0:
        liquidation_score = 1 if liq.data[0]["bullish_liq"] > liq.data[0]["bearish_liq"] else -1
    else:
        liquidation_score = 0

    return {
        "symbol": symbol,
        "orderbook_score": orderbook_score,
        "liquidation_score": liquidation_score
    }

def insert_shortterm_signal(symbol, timeframe, scores):
    """Insert results into ai_signals_shortterm"""
    confidence_score = (scores["orderbook_score"] + scores["liquidation_score"]) / 2 * 100
    direction = "LONG" if scores["orderbook_score"] + scores["liquidation_score"] > 0 else "SHORT"

    row = {
        "symbol": symbol,
        "signal_time": datetime.now(timezone.utc).isoformat(),
        "timeframe": timeframe,
        "orderbook_score": scores["orderbook_score"],
        "liquidation_score": scores["liquidation_score"],
        "confidence_score": confidence_score,
        "direction": direction
    }

    res = sb.table("ai_signals_shortterm").insert(row).execute()
    print(f"[shortterm] {symbol} {timeframe} {direction} ({confidence_score:.1f}%)")
    return res

if __name__ == "__main__":
    symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    for s in symbols:
        scores = get_latest_signal_inputs(s, timeframe="5m")
        insert_shortterm_signal(s, "5m", scores)

