import os 
import time
import re
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

# ========= SIGNAL TYPES =========
MID_LONG_TYPES = [
    "CVD", "DELTA_1H", "DELTA_4H", "DELTA_1D", "DELTA_1W", "UNLOCK", "WHALE_INFLOW"
]

def classify_timeframe(signal_type: str) -> str:
    stype = (signal_type or "").upper()
    if stype in ["CVD", "DELTA_1H", "DELTA_4H", "WHALE_INFLOW"]:
        return "mid_term"
    elif stype in ["DELTA_1D", "DELTA_1W", "UNLOCK"]:
        return "long_term"
    else:
        return "short_term"

def fetch_recent_signals():
    """Fetch only mid/long-term signals within lookback window, capped at 1000 rows"""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)).isoformat()
    result = (
        sb.table("ai_signals_core")   # ✅ use table instead of view
        .select("*")
        .gte("signal_time", cutoff)
        .in_("signal_type", MID_LONG_TYPES)
        .order("signal_time", desc=True)
        .limit(1000)  # prevent timeout
        .execute()
    )
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
        "ai_summary": summary,
        "ai_summary_simple": simple_summary,
        "fdv_adj": fdv_adj,
    }
    sb.table("ai_signals_core").upsert(ai_row).execute()
    print(f"[AI] {row['symbol']} {row['signal_type']} {ai_row['timeframe']} → {label} ({confidence}%) FDV_adj={fdv_adj}")

def score_signal(row):
    fdv = float(row.get("fdv") or 0)
    mcap = float(row.get("market_cap") or 0)
    fdv_adj = 0
    if fdv and mcap:
        ratio = fdv / mcap
        if ratio > 5:
            fdv_adj = -15
        elif ratio > 2:
            fdv_adj = -10
        elif ratio < 1:
            fdv_adj = +5

    prompt = f"""
You are an AI trading analyst. Analyze the following MID/LONG-TERM signal and decide direction + confidence.
Always explain the reasoning clearly.

Signal data:
- Symbol: {row['symbol']}
- Signal type: {row['signal_type']}
- Timeframe: {row.get('timeframe') or classify_timeframe(row.get('signal_type'))}
- Strength value: {row.get('strength_value')}
- Confidence (raw): {row.get("confidence")}
- FDV: {fdv}
- Market Cap: {mcap}
- Days until unlock: {row.get('unlock_days') or "not provided"}
- Whale inflow (USD): {row.get('whale_inflow') or "not provided"}

Weights to apply:
- Mid-term confirmation (1h/4h Delta, CVD) = +40%
- Long-term unlock risk = -20%
- Whale inflow = +10%
- FDV adjustment = {fdv_adj}%

Tasks:
1. Give one-word label: BULLISH / BEARISH / NEUTRAL.
2. Confidence % (0–100), after applying weights above.
3. Short simple summary.
4. Detailed reasoning that:
   - Mentions mid/long explicitly.
   - Explains why it’s bullish/bearish/neutral.
   - If unlock is present, mention days until unlock & % of mcap.
   - If whale inflow is present, mention size and impact.
   - If FDV is high vs MCAP, explain risk impact.
"""

    response = client.chat.completions.create(
        model="gpt-5-mini",
        messages=[{"role": "user", "content": prompt}],
        max_completion_tokens=300,
    )

    text = response.choices[0].message.content.strip()
    lines = text.split("\n")

    label, confidence, simple_summary, detailed_summary = "NEUTRAL", 50, "", text
    for line in lines:
        low = line.lower()
        if "label" in low:
            parts = re.split(r"[:=\-–>]+", line, maxsplit=1)
            if len(parts) > 1:
                label = parts[1].strip().upper().split()[0]
        elif "confidence" in low:
            parts = re.split(r"[:=\-–>]+", line, maxsplit=1)
            if len(parts) > 1:
                val = parts[1].strip().replace("%", "").split()[0]
                try:
                    confidence = int(val)
                except ValueError:
                    pass
        elif "short" in low and not simple_summary:
            parts = re.split(r"[:=\-–>]+", line, maxsplit=1)
            if len(parts) > 1:
                simple_summary = parts[1].strip()
        elif "detail" in low and detailed_summary == text:
            parts = re.split(r"[:=\-–>]+", line, maxsplit=1)
            if len(parts) > 1:
                detailed_summary = parts[1].strip()

    return label, confidence, detailed_summary, simple_summary, fdv_adj

def main():
    while True:
        try:
            signals = fetch_recent_signals()
            print(f"[fetch] {len(signals)} mid/long signals found in last {LOOKBACK_HOURS}h")

            for row in signals:
                try:
                    label, conf, detailed, simple, fdv_adj = score_signal(row)
                    upsert_ai_signal(row, conf, label, detailed, simple, fdv_adj)
                except Exception as e:
                    print(f"[error scoring] {row}: {e}")

        except Exception as e:
            print("Fatal error:", e)

        time.sleep(600)

if __name__ == "__main__":
    main()
