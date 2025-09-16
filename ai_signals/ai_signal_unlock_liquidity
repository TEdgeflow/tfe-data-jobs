import os
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
    """Fetch rows from the final SQL view v_signal_unlock_liquidity_whales"""
    query = sb.table("v_signal_unlock_liquidity_whales").select("*").execute()
    return query.data or []

# ========= AI ENRICH =========
def ai_enrich(signal):
    """Use OpenAI to generate reasoning for the signal"""
    prompt = f"""
    Token: {signal['coin_symbol']}
    Trade Signal: {signal['final_trade_signal']}
    Buy %: {signal['buy_signal_percent']}
    Risk %: {signal['risk_signal_percent']}
    Confidence Score: {signal['confidence_score']}
    Composite Trade Score: {signal['composite_trade_score']}
    Rationale: {signal['rationale']}
    Unlock Risk: {signal['unlock_risk_flag']}

    Data sources:
    - Dropstab Unlock Data: {signal['droptab_url']}
    - Coinglass Heatmap: {signal['coinglass_url']}
    - Coingecko Market Data: {signal['coingecko_url']}

    Write a short 2–3 sentence professional trading insight:
    - Confirm whether BUY/SELL/NEUTRAL makes sense.
    - Mention risk if unlock is near.
    - Use Dropstab/Coinglass context for reasoning (but don’t paste URLs).
    """

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a professional crypto trading analyst."},
            {"role": "user", "content": prompt}
        ],
        max_tokens=200
    )

    return response.choices[0].message.content.strip()

# ========= STORE =========
def store_ai_signal(signal, ai_summary):
    """Insert enriched signal into ai_signals table"""
    row = {
        "id": signal["signal_id"],
        "token_symbol": signal["coin_symbol"],   # ✅ standardized to token_symbol
        "signal_type": signal["signal_type"],
        "final_trade_signal": signal["final_trade_signal"],
        "confidence_score": signal["confidence_score"],
        "signal_strength": signal["signal_strength"],
        "rationale": signal["rationale"],
        "buy_signal_percent": signal["buy_signal_percent"],
        "risk_signal_percent": signal["risk_signal_percent"],
        "composite_trade_score": signal["composite_trade_score"],
        "unlock_risk_flag": signal["unlock_risk_flag"],
        "ai_summary": ai_summary,
        "droptab_url": signal["droptab_url"],
        "coinglass_url": signal["coinglass_url"],
        "coingecko_url": signal["coingecko_url"],
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    sb.table("ai_signals").upsert(row).execute()

# ========= RUN =========
def run_job():
    signals = fetch_signals()
    for sig in signals:
        try:
            ai_summary = ai_enrich(sig)
            store_ai_signal(sig, ai_summary)
            print(f"✅ Stored AI signal for {sig['coin_symbol']}")
        except Exception as e:
            print(f"❌ Error with {sig.get('coin_symbol', 'UNKNOWN')}: {e}")

if __name__ == "__main__":
    run_job()


