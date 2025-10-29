import os
import json
import re
from supabase import create_client, Client
from datetime import datetime, timezone
from openai import OpenAI
import time

# =========================================================
# 1️⃣ ENVIRONMENT SETUP (same as your existing ingestion)
# =========================================================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if not SUPABASE_URL or not SUPABASE_KEY or not OPENAI_API_KEY:
    raise ValueError("❌ Missing SUPABASE_URL, SUPABASE_KEY, or OPENAI_API_KEY in environment variables")

# Create Supabase and OpenAI clients
sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
client = OpenAI(api_key=OPENAI_API_KEY)

# =========================================================
# 2️⃣ PROMPT TEMPLATE
# =========================================================
SUMMARY_PROMPT = """
Summarize this market squeeze signal in 1–2 short sentences (analytical tone, no fluff):
Symbol: {symbol}
Timeframe: {timeframe}
Direction: {direction}
Squeeze Score: {score}
Confidence Level: {confidence} ({confidence_score})
"""

# =========================================================
# 3️⃣ GPT SUMMARIZATION FUNCTION
# =========================================================
def summarize_signal(row):
    """Generate a concise AI summary for a single signal row."""
    prompt = SUMMARY_PROMPT.format(
        symbol=row["symbol"],
        timeframe=row["timeframe"],
        direction=row["squeeze_direction"],
        score=row["squeeze_score"],
        confidence=row["confidence_level"],
        confidence_score=row["confidence_score"]
    )

    try:
        response = client.chat.completions.create(
            model="gpt-4-mini",   # or 'gpt-5-mini' if available in your plan
            messages=[{"role": "user", "content": prompt}],
            temperature=0.4,
            max_tokens=80,
        )

        summary = response.choices[0].message.content.strip()
        summary = re.sub(r"\s+", " ", summary)
        return summary

    except Exception as e:
        print(f"❌ Error summarizing {row['symbol']} {row['timeframe']}: {e}")
        return None

# =========================================================
# 4️⃣ MAIN PIPELINE
# =========================================================
def main():
    print("🚀 Starting signal_fast_engine_ai_summary worker...")

    # 1️⃣ Fetch rows that need summaries
    response = sb.table("signal_fast_engine_ai_summary") \
        .select("*") \
        .is_("ai_summary", "null") \
        .order("signal_time", desc=True) \
        .limit(25) \
        .execute()

    rows = response.data or []

    if not rows:
        print("✅ No new rows to summarize.")
        return

    print(f"🧩 Found {len(rows)} unsummarized rows...")

    # 2️⃣ Loop through each row and summarize
    for row in rows:
        summary = summarize_signal(row)
        if summary:
            print(f"📝 {row['symbol']} {row['timeframe']} → {summary}")

            sb.table("signal_fast_engine_ai_summary").update({
                "ai_summary": summary,
                "last_updated": datetime.now(timezone.utc).isoformat()
            }).eq("id", row["id"]).execute()
        else:
            print(f"⚠️ Skipped {row['symbol']} {row['timeframe']} due to summarization error.")

        # polite delay to avoid rate limits
        time.sleep(1.2)

    print("🎉 AI summarization cycle complete.")

# =========================================================
# 5️⃣ ENTRYPOINT
# =========================================================
if __name__ == "__main__":
    main()
