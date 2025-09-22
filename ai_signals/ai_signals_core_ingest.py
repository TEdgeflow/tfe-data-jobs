import os
import time
from datetime import datetime, timedelta, timezone
from supabase import create_client, Client
from openai import OpenAI

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "24"))  # default = 24h

if not SUPABASE_URL or not SUPABASE_KEY or not OPENAI_API_KEY:
    raise RuntimeError("Missing required environment variables")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
client = OpenAI(api_key=OPENAI_API_KEY)

# ========= HELPERS =========
def map_timeframe(row):
    """Classify timeframe into short, mid, or long term based on signal_type."""
    stype = row.get("signal_type", "").upper()

    if stype in ["LIQUIDATION", "VWAP", "DELTA", "DELTA_5M"]:
        return "short_term"
    elif stype in ["CVD", "DELTA_1H", "DELTA_1D"]:
        return "mid_term"
    elif stype == "UNLOCK":
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
        "direction": label,
        "strength_value": row.get("strength_value"),
        "confidence": confidence,
        "ai_summary": summary,          # detailed version
        "ai_summary_simple": simple_summary,  # simple yes/no/hold
        "timeframe": map_timeframe(row),      # ✅ timeframe classification
    }
    sb.table("ai_signals_core").upsert(ai_row).execute()
    print(f"[AI] {row['symbol']} {row['signal_type']} {map_timeframe(row)} → {label} ({confidence}%)")

def score_signal(row):
    """Send row to GPT for scoring with reasoning"""
    prompt = f"""
You are an AI trading analyst. Analyze the following signal and decide direction + confidence.
Always explain the reasoning clearly.

Signal:
- Symbol: {row['symbol']}
- Type: {row['signal_type']}
- Direction: {row['direction']}
- Strength: {row.get('strength_value')}
- Timeframe: {map_timeframe(row)}
- Notes: {row['notes']}

Apply these weights:
- Short-term alignment (VWAP + Delta + Liquidations) = +30%
- Mid-term confirmation (1h/1d Delta, CVD) = +40%
- Long-term unlock risk = -20%
- Whale inflow = +10%

Output strictly in this format:
Label: BULLISH/BEARISH/NEUTRAL
Confidence: NN%
Short: one-sentence summary
Detailed: detailed reasoning
"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",  # upgrade to gpt-5 if enabled
        messages=[{"role": "user", "content": prompt}],
        max_tokens=250,
    )

    text = response.choices[0].message["content"].strip()

    # naive parsing
    lines = text.split("\n")
    label = next((l.split(":")[-1].strip().upper() for l in lines if "Label" in l), "NEUTRAL")
    confidence = next((int(l.split(":")[-1].strip().replace("%", "")) for l in lines if "Confidence" in l), 50)
    simple_summary = next((l.split(":")[-1].strip() for l in lines if "Short" in l), "")
    detailed_summary = next((l.split(":")[-1].strip() for l in lines if "Detailed" in l), text)

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





