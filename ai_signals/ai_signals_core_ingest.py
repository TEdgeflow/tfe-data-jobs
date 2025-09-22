import os
import re
import time
from datetime import datetime, timedelta, timezone
from supabase import create_client, Client
from openai import OpenAI

# ============ ENV ============
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "24"))   # pull last N hours of signals
POLL_SECONDS   = int(os.getenv("POLL_SECONDS", "600"))     # run every N seconds
OPENAI_MODEL   = os.getenv("OPENAI_MODEL", "gpt-5-mini")   # or "gpt-4o-mini"

if not SUPABASE_URL or not SUPABASE_KEY or not OPENAI_API_KEY:
    raise RuntimeError("Missing SUPABASE_URL / SUPABASE_KEY / OPENAI_API_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
client = OpenAI(api_key=OPENAI_API_KEY)


# ============ DATA HELPERS ============
def fetch_recent_signals(limit: int = 1000):
    """
    Pull recent rows from the consolidated view v_ai_signals_core.
    We cap the range to avoid statement timeouts on large tables.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)).isoformat()

    # SELECT * FROM v_ai_signals_core WHERE signal_time >= cutoff
    # ORDER BY signal_time DESC LIMIT <limit>
    q = (
        sb.table("v_ai_signals_core")
          .select("*")
          .gte("signal_time", cutoff)
          .order("signal_time", desc=True)
    )

    # Supabase Python client uses .range(start, end) for limit
    # (end is inclusive), so .range(0, limit-1).
    res = q.range(0, max(0, limit - 1)).execute()
    return res.data or []


def upsert_ai_signal(row, confidence, label, detailed_summary, simple_summary):
    """
    Upsert into ai_signals_core (your clean table).
    Assumes you’ve created a unique constraint on (symbol, signal_time, signal_type)
    so we don’t double-score the same row on repeated runs.
    """
    payload = {
        "symbol": row["symbol"],
        "signal_time": row["signal_time"],
        "signal_type": row["signal_type"],
        "direction": label,
        "strength_value": row.get("strength_value"),
        "confidence": confidence,
        "ai_summary": detailed_summary,         # detailed reasoning
        "ai_summary_simple": simple_summary,     # one-liner
        "timeframe": row.get("timeframe"),
    }
    sb.table("ai_signals_core").upsert(payload).execute()
    print(f"[AI] {row['symbol']} {row['signal_type']} {row.get('timeframe')} → {label} ({confidence}%)")


# ============ LLM SCORING ============
def build_prompt(row) -> str:
    """
    Prompt with explicit weighting instructions + strict output format.
    """
    return f"""
You are an AI trading analyst. Analyze the following signal and decide direction + confidence.
Always explain the reasoning clearly and concisely.

Signal:
- Symbol: {row['symbol']}
- Type: {row['signal_type']}
- Incoming Direction: {row.get('direction')}
- Strength: {row.get('strength_value')}
- Timeframe: {row.get('timeframe')}
- Notes: {row['notes']}

Apply these weights when forming your view:
- Short-term alignment (VWAP + Delta + Liquidations) = +30%
- Mid-term confirmation (1h/1d Delta, CVD) = +40%
- Long-term unlock risk (large unlocks soon) = −20%
- Whale inflow presence (Nansen inflow) = +10%

Output strictly in this format (one item per line):
Label: BULLISH/BEARISH/NEUTRAL
Confidence: NN%
Short: one-sentence summary
Detailed: detailed reasoning
""".strip()


def score_signal(row):
    """
    Ask the model to score a single consolidated signal.
    Returns: (label, confidence_int, detailed_summary, simple_summary)
    """
    prompt = build_prompt(row)

    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[{"role": "user", "content": prompt}],
        max_tokens=300,
        temperature=0.2,
    )

    # Use .content (not dict index) to avoid ChatCompletionMessage subscripting error
    text = (resp.choices[0].message.content or "").strip()

    # Robust parsing via regex
    # Label
    m_label = re.search(r"Label\s*:\s*(BULLISH|BEARISH|NEUTRAL)", text, re.I)
    label = (m_label.group(1).upper() if m_label else (row.get("direction") or "NEUTRAL"))

    # Confidence
    m_conf = re.search(r"Confidence\s*:\s*([0-9]{1,3})\s*%?", text, re.I)
    try:
        confidence = int(m_conf.group(1)) if m_conf else 50
        confidence = max(0, min(100, confidence))
    except Exception:
        confidence = 50

    # Short
    m_short = re.search(r"Short\s*:\s*(.+)", text, re.I)
    simple_summary = (m_short.group(1).strip() if m_short else "").strip()

    # Detailed (multi-line to end)
    m_det = re.search(r"Detailed\s*:\s*(.+)", text, re.I | re.S)
    detailed_summary = (m_det.group(1).strip() if m_det else text).strip()

    # Fallbacks if model returned blank lines
    if not simple_summary:
        simple_summary = f"{label.title()} — {row.get('signal_type')} @ {row.get('timeframe')}"
    if not detailed_summary:
        detailed_summary = f"Model returned minimal reasoning. Raw text:\n{text}"

    return label, confidence, detailed_summary, simple_summary


# ============ MAIN LOOP ============
def main():
    while True:
        try:
            signals = fetch_recent_signals(limit=1000)
            print(f"[fetch] {len(signals)} signals found in last {LOOKBACK_HOURS}h")

            for row in signals:
                try:
                    label, conf, detailed, simple = score_signal(row)
                    upsert_ai_signal(row, conf, label, detailed, simple)
                except Exception as e:
                    # Keep going if a single row fails
                    print(f"[error scoring] {row.get('symbol')} {row.get('signal_type')} -> {e}")

        except Exception as e:
            print("Fatal error:", e)

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()


