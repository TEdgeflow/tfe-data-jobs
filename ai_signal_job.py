import os
import re
from supabase import create_client
from openai import OpenAI
from datetime import datetime, timedelta

# === Supabase setup ===
sb = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

# === OpenAI setup ===
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def run_ai_signals():
    # 1. Get signals in the last 6 hours
    since = (datetime.utcnow() - timedelta(hours=6)).isoformat()
    signals = sb.table("v_signal_master").select("*").gte("signal_time", since).execute().data

    print(f"[ai_signals] Found {len(signals)} signals since {since}")

    for sig in signals:
        # 2. Build AI prompt
        prompt = f"""
        Token: {sig['token_symbol']}
        Signal type: {sig['signal_type']}
        Signal strength: {sig['signal_strength']}
        Metric: {sig['key_metric']}

        Please output:
        - Confidence score (0-100)
        - One-sentence rationale
        """

        try:
            response = client.chat.completions.create(
                model="gpt-5-mini",
                messages=[{"role": "user", "content": prompt}]
            )

            # Confirm which model is used
            print(f"[ai_signals] Model used: {response.model}")

            content = response.choices[0].message.content
            print(f"[ai_signals] Raw AI response: {content}")

            # Safer parsing for confidence score
            match = re.search(r"(\d{1,3})", content)
            confidence = int(match.group(1)) if match else 50
            rationale = content.strip()

            # 3. Insert into ai_signals table
            sb.table("ai_signals").insert({
                "token_symbol": sig['token_symbol'],
                "signal_type": sig['signal_type'],
                "confidence_score": confidence,
                "rationale": rationale,
                "created_at": sig['signal_time']
            }).execute()

            print(f"[ai_signals] Inserted {sig['token_symbol']} with confidence {confidence}")

        except Exception as e:
            print(f"[ai_signals] Error processing {sig.get('token_symbol')}: {e}")

if __name__ == "__main__":
    print("[ai_signals] Job started")
    run_ai_signals()
    print("[ai_signals] Job finished")


