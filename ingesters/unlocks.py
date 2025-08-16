# ingesters/unlocks.py
import os, requests
from datetime import datetime, timezone

SRC = os.getenv("UNLOCKS_SOURCE", "tokenunlocks")   # or 'cryptorank'/'dropstab'
API = os.getenv("UNLOCKS_API_BASE", "")             # set in Railway env
KEY = os.getenv("UNLOCKS_API_KEY", "")              # set if needed

def iso(ts):
    if isinstance(ts, (int, float)):
        return datetime.fromtimestamp(ts/1000, tz=timezone.utc).isoformat()
    return ts  # assume already ISO

def fetch_unlocks_for(symbol: str) -> list[dict]:
    """
    Replace the URL/params below with the real provider API.
    Expected normalized output keys: symbol, unlock_time, unlock_type, amount_tokens, amount_usd, pct_circ, source
    """
    headers = {"Authorization": f"Bearer {KEY}"} if KEY else {}
    # Example placeholder endpoint shape:
    url = f"{API}/unlocks?symbol={symbol}"
    r = requests.get(url, headers=headers, timeout=20)
    r.raise_for_status()
    items = r.json() or []
    rows = []
    for it in items:
        rows.append({
            "symbol": symbol,
            "unlock_time": iso(it.get("unlock_time") or it.get("date") or it.get("ts")),
            "unlock_type": (it.get("type") or "OTHER").upper(),
            "amount_tokens": it.get("amount_tokens"),
            "amount_usd": it.get("amount_usd"),
            "pct_circ": it.get("pct_circ"),
            "source": SRC
        })
    return rows
