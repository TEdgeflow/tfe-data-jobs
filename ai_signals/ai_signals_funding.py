import os
import re
from supabase import create_client
from openai import OpenAI
from datetime import datetime, timedelta

# === Supabase setup ===
sb = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

# === OpenAI setup ===
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# === Config ===
LOOKBACK_HR = int(os.getenv("SIGNAL_LOOKBACK_HR", "6"))  # default 6 hours

def run_ai_signals():
    # 1. Fetch recent signals from funding squeeze view
    since = (datetime.utcnow() - timedelta(hours=LOOKBACK_HR)).isoformat()
    signals = (
        sb.table("v_signal_funding_squeeze")
        .select("*")
        .gte("created_at", since)
        .execute()
        .data
    )

    print(f"[ai_signals_funding] Found {len(signals)} signals since {since}")

    for sig in signals:
        prompt = f"""
        Token: {sig['symbol']}
        Signal type: {sig['signal_type']}
        SQL Confidence: {sig['confidence_score']}
        SQL Signal Strength: {sig['signal_strength']}
        SQL Rationale: {sig['rationale']}

        Please output:
        - Adjusted confidence score (0-100)
        - Improved one-sentence rationale
        """

        try:
            response = client.chat.completions.create(
                model="gpt-5-mini",
                messages=[{"role": "user", "content": prompt}]
            )

            content = response.choices[0].message.content
            print(f"[ai_signals_funding] AI raw response: {content}")

            # Extract confidence score
            match = re.search(r"(\d{1,3})", content)
            confidence = int(match.group(1)) if match else int(sig["confidence_score"] * 100)
            rationale = content.strip()

            # ✅ Safe insert: prevents duplicates on same signal_id
            sb.table("ai_signals").insert({
                "id": sig['signal_id'],  # reuse UUID from view
                "symbol": sig['symbol'],
                "signal_type": sig['signal_type'],
                "signal_category": sig['signal_category'],
                "confidence_score": confidence,
                "signal_strength": sig['signal_strength'],
                "rationale": rationale,
                "created_at": sig['created_at']
            }).execute()

            print(f"[ai_signals_funding] Inserted {sig['symbol']} with confidence {confidence}")

        except Exception as e:
            print(f"[ai_signals_funding] Error for {sig.get('symbol')}: {e}")

if __name__ == "__main__":
    print("[ai_signals_funding] Job started")
    run_ai_signals()
    print("[ai_signals_funding] Job finished")
