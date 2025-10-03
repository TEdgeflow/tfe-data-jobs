import os
from supabase import create_client
from datetime import datetime, timezone, date

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

def normalize_symbol(symbol: str, target: str) -> str:
    """Convert Binance symbol (BTCUSDT) to correct format for other tables."""
    if target in ["nansen_whaleflows", "v_droptabs_unlocks"]:
        return symbol.replace("USDT", "")   # e.g., BTCUSDT -> BTC
    return symbol   # keep as-is for Binance tables

# ========= GET DATA =========
def get_daybias_inputs(symbol: str, timeframe: str = "1h"):
    vwap = sb.table("binance_vwap_agg").select("*") \
        .eq("symbol", symbol).eq("timeframe", timeframe) \
        .order("bucket_start", desc=True).limit(1).execute()

    delta = sb.table("v_signal_delta").select("*") \
        .eq("symbol", symbol).eq("timeframe", timeframe) \
        .order("signal_time", desc=True).limit(1).execute()

    cvd = sb.table("v_signal_cvd").select("*") \
        .eq("symbol", symbol).eq("timeframe", timeframe) \
        .order("signal_time", desc=True).limit(1).execute()

    ob = sb.table("binance_orderbook_agg_1h").select("*") \
        .eq("symbol", normalize_symbol(symbol, "binance_orderbook_agg_1h")) \
        .order("bucket_1h", desc=True).limit(1).execute()

    liq = sb.table("v_liquidation_agg").select("*") \
        .eq("symbol", normalize_symbol(symbol, "v_liquidation_agg")) \
        .order("last_update", desc=True).limit(1).execute()

    inflow = sb.table("nansen_whaleflows").select("*") \
        .eq("token", normalize_symbol(symbol, "nansen_whaleflows")) \
        .order("ts", desc=True).limit(1).execute()

    unlock = sb.table("v_droptabs_unlocks").select("*") \
        .eq("coin_symbol", normalize_symbol(symbol, "v_droptabs_unlocks")) \
        .limit(1).execute()

    # ========= SCORES =========
    vwap_score = 1 if vwap.data and vwap.data[0]["vwap"] < vwap.data[0].get("close_price", 0) else 0
    delta_score = 1 if delta.data and delta.data[0]["strength_value"] > 0 else 0
    cvd_score = 1 if cvd.data and cvd.data[0]["strength_value"] > 0 else 0
    orderbook_score = 1 if ob.data and ob.data[0]["bid_vol10"] > ob.data[0]["ask_vol10"] else 0
    liquidation_score = 1 if liq.data and liq.data[0]["long_liquidations"] > liq.data[0]["short_liquidations"] else 0
    volume_score = 1 if vwap.data and vwap.data[0]["volume_quote"] > 5_000_000 else 0
    inflow_score = 1 if inflow.data and inflow.data[0]["inflow_usd"] > 100_000 else 0
    unlock_score = 1 if unlock.data and unlock.data[0]["days_until_unlock"] <= 30 else 0

    scores = {
        "symbol": symbol,
        "vwap_score": vwap_score,
        "delta_score": delta_score,
        "cvd_score": cvd_score,
        "orderbook_score": orderbook_score,
        "liquidation_score": liquidation_score,
        "volume_score": volume_score,
        "whale_inflow_score": inflow_score,
        "unlock_risk_score": unlock_score,
    }

    return scores, vwap, delta, cvd

# ========= SCORING =========
def calculate_confidence(scores):
    mapping = {
        "vwap_score": "vwap",
        "delta_score": "delta",
        "cvd_score": "cvd",
        "orderbook_score": "orderbook",
        "liquidation_score": "liquidation",
        "volume_score": "volume",
        "whale_inflow_score": "whale_inflow",
        "unlock_risk_score": "unlock_risk"
    }
    total = sum(scores[k] * WEIGHTS[mapping[k]] for k in mapping)
    return round(total * 100, 2)

def get_direction(scores):
    bullish = scores["vwap_score"] + scores["delta_score"] + scores["cvd_score"] + scores["orderbook_score"]
    bearish = scores["liquidation_score"] + scores["volume_score"]
    return "LONG" if bullish >= bearish else "SHORT"

# ========= INSERT =========
def insert_signal(symbol, scores, vwap=None, delta=None, cvd=None):
    confidence = calculate_confidence(scores)
    direction = get_direction(scores)

    # derive signal_date
    signal_date = None
    if vwap and vwap.data and "bucket_start" in vwap.data[0]:
        signal_date = vwap.data[0]["bucket_start"][:10]
    elif delta and delta.data and "signal_time" in delta.data[0]:
        signal_date = delta.data[0]["signal_time"][:10]
    elif cvd and cvd.data and "signal_time" in cvd.data[0]:
        signal_date = cvd.data[0]["signal_time"][:10]
    else:
        signal_date = date.today().isoformat()

    row = {
        "symbol": symbol,
        "vwap_trend_score": scores.get("vwap_score", 0),
        "delta_trend_score": scores.get("delta_score", 0),
        "cvd_alignment_score": scores.get("cvd_score", 0),
        "orderbook_score": scores.get("orderbook_score", 0),
        "liquidation_bias_score": scores.get("liquidation_score", 0),
        "volume_rsi_score": scores.get("volume_score", 0),
        "whale_inflow_score": scores.get("whale_inflow_score", 0),
        "unlock_bias_score": scores.get("unlock_risk_score", 0),
        "confidence_score": confidence,
        "bias": direction,
        "signal_date": signal_date,   # ✅ required by schema
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    sb.table("ai_signals_daybias").insert(row).execute()
    print(f"[daybias_signal] {symbol} {direction} {confidence}% (date={signal_date})")



# ========= MAIN =========
if __name__ == "__main__":
    # get all distinct symbols from vwap table
    symbols_resp = sb.table("binance_vwap_agg").select("symbol").execute()
    all_symbols = sorted({row["symbol"] for row in symbols_resp.data if "symbol" in row})

    print(f"Processing {len(all_symbols)} symbols...")

    for s in all_symbols:
        scores, vwap, delta, cvd = get_daybias_inputs(s, timeframe="1h")
        insert_signal(s, scores, vwap=vwap, delta=delta, cvd=cvd)






