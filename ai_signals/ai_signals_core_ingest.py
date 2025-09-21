import os
import openai
from supabase import create_client, Client

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if not SUPABASE_URL or not SUPABASE_KEY or not OPENAI_API_KEY:
    raise RuntimeError("Missing SUPABASE_URL, SUPABASE_KEY or OPENAI_API_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
openai.api_key = OPENAI_API_KEY

# ========= WEIGHTING RULES =========
WEIGHTS = {
    "VWAP": 0.30,
    "DELTA": 0.30,
    "DELTA_5M": 0.30,
    "LIQUIDATION": 0.30,
    "CVD": 0.40,
    "DELTA_1H": 0.40,
    "DELTA_1D": 0.40,
    "UNLOCK": -0.20,
    "UNLOCK_30D": -0.20,
    "UNLOCK_60D": -0.20,
    "UNLOCK_90D": -0.20,
    "UNLOCK_180D": -0.20,
    "UNLOCK_280D": -0.20,
    "WHALE_INFLOW": 0.10,
}

# ========= HELPERS =========
def compute_confidence(row):
    base = WEIGHTS.get(row["signal_type"], 0)
    val = abs(row.get("strength_value") or 0)
    return round(min(100, max(0, 50 + (val * base * 10))), 2)

def ask_openai(row, confidence):
    """Ask GPT for summary and decision"""
    prompt = f"""
    You are an AI trading assistant. Analyze the following signal and return a
    clear decision (BUY, SELL, or NEUTRAL) with 1-line reasoning.

    Signal:
    - Symbol: {row['symbol']}
    - Type: {row['signal_type']}
    - Direction: {row['direction']}
    - Strength: {row['strength_value']}
    - Notes: {row['notes']}
    - Confidence: {confidence}%

    Respond in JSON with fields: "label", "summary".
    """

    resp = openai.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )
    content = resp.choices[0].message["content"]
    try:
        import json
        parsed = json.loads(content)
        return parsed.get("label", "NEUTRAL"), parsed.get("summary", content)
    except Exception:
        return "NEUTRAL", content

def upsert_ai_signal(row, confidence, label, summary):
    ai_row = {
        "symbol": row["symbol"],
        "signal_time": row["signal_time"],
        "signal_type": row["signal_type"],
        "direction": label,
        "strength_value": row.get("strength_value"),
        "confidence": confidence,
        "ai_summary": summary,
    }
    sb.table("ai_signals_core").upsert(ai_row).execute()
    print(f"[AI] {row['symbol']} {row['signal_type']} → {label} ({confidence}%)")

# ========= MAIN =========
def main():
    rows = sb.table("v_ai_signals_core").select("*").limit(200).execute().data
    if not rows:
        print("No signals found.")
        return

    for row in rows:
        try:
            conf = compute_confidence(row)
            label, summary = ask_openai(row, conf)
            upsert_ai_signal(row, conf, label, summary)
        except Exception as e:
            print("[error]", row.get("symbol"), e)

if __name__ == "__main__":
    main()
