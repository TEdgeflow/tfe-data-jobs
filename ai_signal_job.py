import os
from supabase import create_client
from openai import OpenAI
from datetime import datetime, timedelta

# Supabase setup
sb = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

# OpenAI setup
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# 1. Get yesterday’s signals from v_signal_master
yesterday = (datetime.utcnow() - timedelta(days=1)).isoformat()
signals = sb.table("v_signal_master").select("*").gte("signal_time", yesterday).execute().data

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

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}]
    )

    # Parse AI response (very simple version)
    content = response.choices[0].message.content
    try:
        confidence = int([s for s in content.split() if s.isdigit()][0])
    except:
        confidence = 50
    rationale = content

    # 3. Insert into ai_signals table
    sb.table("ai_signals").insert({
        "token_symbol": sig['token_symbol'],
        "signal_type": sig['signal_type'],
        "confidence_score": confidence,
        "rationale": rationale,
        "created_at": sig['signal_time']
    }).execute()
