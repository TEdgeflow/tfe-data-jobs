import os
import json
import re
from supabase import create_client, Client
from datetime import datetime, timezone
from openai import OpenAI

# ========= ENV =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
client = OpenAI(api_key=OPENAI_API_KEY)

# ========= FETCH =========
def fetch_signals():
    """Fetch rows from the SQL view v_signal_unlock_liquidity_whales"""
    query = sb.table("v_signal_unlock_liquidity_whales").select("*").execute()
    return query.data or []

# ========= JSON PARSE SAFE =========
def safe_json_parse(ai_output):
    try:
        cleaned = ai_output.strip()
        # Strip code fences like ```json ... ```
        cleaned = re.sub(r"^```[a-zA-Z]*|```$", "", cleaned, flags=re.MULTILINE).strip()
        return json.loads(cleaned)
    except Exception as e:
        print(f"⚠️ JSON parse failed, fallback used: {e}")
        return {
            "confidence_score": 50,
            "signal_strength": "Medium",
            "rationale": ai_output,
            "final_trade_signal": "NEUTRAL",
        }

# ========= AI ENRICH =========
def ai_enrich(signal):
    """Use OpenAI to generate reasoning for the signal"""
    prompt = f"""
    Token: {signal['coin_symbol']}
    Signal type: {signal['signal_type']}
    Days until unlock: {signal['days_until_unlock']}
    Unlocked %: {signal['unlocked_percent']}
    Nearest bid liquidity: {signal['liquidity_nearest_bid']}
    Nearest ask liquidity: {signal['liquidity_nearest_ask']}
    Whale inflow (USD): {signal['inflow_usd']}
    Whale netflow (USD): {signal['netflow_usd']}

    Please output in JSON format:
    {{
      "confidence_score": (0-100 number),
      "signal_strength": "High/Medium/Low",
      "rationale": "one-sentence rationale",
      "final_trade_signal": "BUY/SELL/NEUTRAL"
    }}
    """

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=300
    )

    return response.choices[0].message.content.strip()

# ========= STORE =========
def store_ai_signal(signal, ai_json):
    """Insert enriched signal into ai_signals table"""
    parsed = safe_json_parse(ai_json)

    row = {
        "token_symbol": signal["coin_symbol"],
        "signal_type": signal["signal_type"],
        "confidence_score": parsed.get("confidence_score", 50),
        "signal_strength": parsed.get("signal_strength", "Medium"),
        "rationale": parsed.get("rationale", ""),
        "final_trade_signal": parsed.get("final_trade_signal", "NEUTRAL"),
        "droptab_url": signal.get("droptab_url"),
        "coinglass_url": signal.get("coinglass_url"),
        "coingecko_url": signal.get("coingecko_url"),
        "created_at": datetime.now(timezone.utc).isoformat()
    }

    print(f"⬆️ Upserting row into ai_signals: {row}")
    sb.table("ai_signals").upsert(row).execute()
    print(f"✅ Stored AI signal for {signal['coin_symbol']}")

# ========= RUN =========
def run_job():
    signals = fetch_signals()
    for sig in signals:
        try:
            ai_json = ai_enrich(sig)
            store_ai_signal(sig, ai_json)
        except Exception as e:
            print(f"❌ Error with {sig.get('coin_symbol', 'UNKNOWN')}: {e}")

if __name__ == "__main__":
    run_job()



