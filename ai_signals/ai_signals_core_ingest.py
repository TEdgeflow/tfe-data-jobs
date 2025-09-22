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
        "timeframe": row.get("timeframe"),
    }
    sb.table("ai_signals_core").upsert(ai_row).execute()
    print(f"[AI] {row['symbol']} {row['signal_type']} {row.get('timeframe')} → {label} ({confidence}%)")

def score_signal(row):
    """Send row to GPT for scoring"""
    prompt = f"""
You are an AI trading analyst. Analyze the signal and decide direction + confidence.

Signal:
- Symbol: {row['symbol']}
- Type: {row['signal_type']}
- Direction: {row['direction']}
- Strength: {row.get('strength_value')}
- Timeframe: {row.get('timeframe')}
- Notes: {row['notes']}

Give:
1. One-word label: BULLISH / BEARISH / NEUTRAL
2. Confidence % (0-100)
3. Short simple summary (e.g. "Bullish pressure, align with 1h delta")
4. Detailed summary with reasoning.
"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",  # can change to gpt-5-mini if enabled
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

