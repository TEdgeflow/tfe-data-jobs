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

def upsert_ai_signal(row, confidence, label, summary, simple_summary):
    ai_row = {
        "symbol": row["symbol"],
        "signal_time": row["signal_time"],
        "signal_type": row["signal_type"],
        "timeframe": row.get("timeframe") or classify_timeframe(row.get("signal_type")),
        "direction": label,
        "strength_value": row.get("strength_value"),
        "confidence": confidence,
        "ai_summary": summary,              # detailed version
        "ai_summary_simple": simple_summary, # simple one-liner
    }
    sb.table("ai_signals_core").upsert(ai_row).execute()
    print(f"[AI] {row['symbol']} {row['signal_type']} {ai_row['timeframe']} → {label} ({confidence}%)")

def score_signal(row):
    """Send row to GPT for scoring"""
    prompt = f"""
You are an AI trading analyst. Analyze the following signal and decide direction + confidence. Always explain the reasoning clearly.

Signal data:
- Symbol: {row['symbol']}
- Signal type: {row['signal_type']}
- Timeframe: {row.get('timeframe') or classify_timeframe(row.get('signal_type'))}
- Strength value: {row.get('strength_value')}
- Notes: {row.get('notes')}
- FDV: {row.get('fdv') or "not provided"}
- Days until unlock: {row.get('unlock_days') or "not provided"}
- Whale inflow (USD): {row.get('whale_inflow') or "not provided"}

Tasks:
1. Give one-word label: BULLISH / BEARISH / NEUTRAL.
2. Confidence % (0–100).
3. Short simple summary (e.g., "Bullish pressure, align with 1h delta").
4. Detailed reasoning that:
   - Explains why it’s bullish/bearish/neutral.
   - Mentions timeframe context (short/mid/long).
   - If unlock is present, mention days until unlock & % of mcap.
   - If whale inflow is present, mention size and impact.
   - If delta or CVD are flat/rising/falling, interpret that.
   - Include any conflicts (e.g. bearish liquidation but bullish inflow).
    """

    response = client.chat.completions.create(
        model="gpt-5-mini",   # ✅ using gpt-5-mini as you confirmed
        messages=[{"role": "user", "content": prompt}],
        max_tokens=300,
    )

    text = response.choices[0].message["content"].strip()
    # simple parsing
    lines = text.split("\n")
    label = next((l.split(":")[-1].strip().upper() for l in lines if "LABEL" in l.upper()), "NEUTRAL")
    confidence = next((int(l.split(":")[-1].strip().replace("%", "")) for l in lines if "CONFIDENCE" in l.upper()), 50)
    simple_summary = next((l.split(":")[-1].strip() for l in lines if "SHORT" in l.upper()), "")
    detailed_summary = next((l.split(":")[-1].strip() for l in lines if "DETAIL" in l.upper()), text)

    return label, confidence, detailed_summary, simple_summary

# ========= MAIN LOOP =========
def main():
    while True:
        try:
            signals = fetch_recent_signals()
            print(f"[fetch] {len(signals)} signals found in last {LOOKBACK_HOURS}h")

            for row in signals:
                try:
                    label, conf, detailed, simple = score_signal(row)
                    upsert_ai_signal(row, conf, label, detailed, simple)
                except Exception as e:
                    print(f"[error scoring] {row}: {e}")

        except Exception as e:
            print("Fatal error:", e)

        time.sleep(600)  # run every 10 minutes

if __name__ == "__main__":
    main()







