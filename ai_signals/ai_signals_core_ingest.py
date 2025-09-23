import os
import time
from datetime import datetime, timedelta, timezone
from supabase import create_client, Client
from openai import OpenAI

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "24"))  # default 24h

if not SUPABASE_URL or not SUPABASE_KEY or not OPENAI_API_KEY:
    raise RuntimeError("Missing required environment variables")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
client = OpenAI(api_key=OPENAI_API_KEY)

# ========= HELPERS =========
def classify_timeframe(signal_type: str) -> str:
    """Classify timeframe into short, mid, or long term based on signal_type."""
    stype = (signal_type or "").upper()
    if stype in ["LIQUIDATION", "VWAP", "DELTA", "DELTA_5M"]:
        return "short_term"
    elif stype in ["CVD", "DELTA_1H", "DELTA_4H", "WHALE_INFLOW"]:
        return "mid_term"
    elif stype in ["DELTA_1D", "DELTA_1W", "UNLOCK"]:
        return "long_term"
    else:
        return "short_term"  # fallback

def fetch_recent_signals():
    """Fetch signals from v_ai_signals_core within lookback window"""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)).isoformat()
    query = sb.table("v_ai_signals_core").select("*").gte("signal_time", cutoff)
    result = query.execute()
    return result.data or []

def upsert_ai_signal(row, confidence, label, summary, simple_summary, fdv_adj):
    ai_row = {
        "symbol": row["symbol"],
        "signal_time": row["signal_time"],
        "signal_type": row["signal_type"],
        "timeframe": row.get("timeframe") or classify_timeframe(row.get("signal_type")),
        "direction": label,
        "strength_value": row.get("strength_value"),
        "confidence": confidence,
        "ai_summary": summary,               # detailed version
        "ai_summary_simple": simple_summary, # simple one-liner
        "fdv_adj": fdv_adj,                  # ✅ new column
    }
    sb.table("ai_signals_core").upsert(ai_row).execute()
    print(f"[AI] {row['symbol']} {row['signal_type']} {ai_row['timeframe']} → {label} ({confidence}%) FDV_adj={fdv_adj}")

def score_signal(row):
    """Send row to GPT for scoring"""

    # normalize strength_value to 0–100
    strength_value = float(row.get("strength_value", 0))
    strength_norm = min(100, max(0, round(strength_value / 1000, 2)))

    # FDV adjustment
    fdv = float(row.get("fdv") or 0)
    mcap = float(row.get("market_cap") or 0)
    fdv_adj = 0
    if fdv and mcap:
        ratio = fdv / mcap
        if ratio > 5:      # very high FDV vs MCAP
            fdv_adj = -15
        elif ratio > 2:    # moderately high
            fdv_adj = -10
        elif ratio < 1:    # FDV below MCAP (rare)
            fdv_adj = +5

    prompt = f"""
You are an AI trading analyst. Analyze the following signal and decide direction + confidence.
Always explain the reasoning clearly.

Signal data:
- Symbol: {row['symbol']}
- Signal type: {row['signal_type']}
- Timeframe: {row.get('timeframe') or classify_timeframe(row.get('signal_type'))}
- Strength value: {row.get('strength_value')}
- Confidence (raw): {row.get("confidence")}
- Delta/Volume info: {row.get("delta_usd")}
- Notes: {row.get('notes')}
- FDV: {fdv}
- Market Cap: {mcap}
- Days until unlock: {row.get('unlock_days') or "not provided"}
- Whale inflow (USD): {row.get('whale_inflow') or "not provided"}

Weights to apply:
- Short-term alignment (VWAP + Delta + Liquidations) = +30%
- Mid-term confirmation (1h/4h/1d Delta, CVD) = +40%
- Long-term unlock risk = -20%
- Whale inflow = +10%
- FDV adjustment = {fdv_adj}%

Tasks:
1. Give one-word label: BULLISH / BEARISH / NEUTRAL.
2. Confidence % (0–100), after applying weights above.
3. Short simple summary (e.g., "Bullish pressure, align with 1h delta").
4. Detailed reasoning that:
   - Mentions timeframe (short/mid/long explicitly).
   - Explains why it’s bullish/bearish/neutral.
   - If unlock is present, mention days until unlock & % of mcap.
   - If whale inflow is present, mention size and impact.
   - If FDV is high vs MCAP, explain how it increases/decreases risk.
   - Include conflicts (e.g., bearish liquidation but bullish inflow).
"""

    response = client.chat.completions.create(
        model="gpt-5-mini",
        messages=[{"role": "user", "content": prompt}],
        max_completion_tokens=300,
    )

   text = response.choices[0].message.content.strip()
    lines = text.split("\n")
    label = next((l.split(":")[-1].strip().upper() for l in lines if "LABEL" in l.upper()), "NEUTRAL")
    confidence = next((int(l.split(":")[-1].strip().replace("%", "")) for l in lines if "CONFIDENCE" in l.upper()), 50)
    simple_summary = next((l.split(":")[-1].strip() for l in lines if "SHORT" in l.upper()), "")
    detailed_summary = next((l.split(":")[-1].strip() for l in lines if "DETAIL" in l.upper()), text)

    return label, confidence, detailed_summary, simple_summary, fdv_adj

# ========= MAIN LOOP =========
def main():
    while True:
        try:
            signals = fetch_recent_signals()
            print(f"[fetch] {len(signals)} signals found in last {LOOKBACK_HOURS}h")

            for row in signals:
                try:
                    label, conf, detailed, simple, fdv_adj = score_signal(row)
                    upsert_ai_signal(row, conf, label, detailed, simple, fdv_adj)
                except Exception as e:
                    print(f"[error scoring] {row}: {e}")

        except Exception as e:
            print("Fatal error:", e)

        time.sleep(600)  # run every 10 minutes

if __name__ == "__main__":
    main()









