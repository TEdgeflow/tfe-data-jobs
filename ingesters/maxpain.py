# ingesters/maxpain.py
import requests, math
from datetime import datetime, timezone

DERIBIT = "https://www.deribit.com/api/v2/public"

def get_deribit_instruments(currency="BTC"):
    r = requests.get(f"{DERIBIT}/get_instruments", params={"currency": currency, "kind": "option", "expired": False}, timeout=20)
    r.raise_for_status()
    return r.json()["result"]

def get_chain(currency="BTC"):
    instr = get_deribit_instruments(currency)
    # group by expiration date
    by_exp = {}
    for i in instr:
        exp = datetime.fromtimestamp(i["expiration_timestamp"]/1000, tz=timezone.utc).date()
        by_exp.setdefault(exp, []).append(i["instrument_name"])
    return by_exp

def get_book_summaries(names: list[str]):
    out = []
    for name in names:
        r = requests.get(f"{DERIBIT}/get_book_summary_by_instrument", params={"instrument_name": name}, timeout=10)
        r.raise_for_status()
        res = r.json()["result"]
        if res:
            out.append(res[0])
    return out

def compute_max_pain_for_exp(currency="BTC", venue="deribit", expiration=None):
    """
    Very simplified: approximate total payout by strike using OI.
    """
    chain = get_chain(currency)
    if expiration not in chain:
        return None
    summaries = get_book_summaries(chain[expiration])
    # collect per-strike call/put OI * strike as rough payout proxy
    strikes = {}
    for s in summaries:
        instr = s["instrument_name"]  # e.g., BTC-30AUG24-60000-C
        parts = instr.split("-")
        strike = float(parts[2]); kind = parts[3]  # C or P
        oi = s.get("open_interest", 0) or 0
        strikes.setdefault(strike, {"C":0.0, "P":0.0})
        strikes[strike][kind] += float(oi)
    # brute force max pain: minimize total intrinsic value to holders
    best_strike, best_cost = None, float("inf")
    all_strikes = sorted(strikes.keys())
    for S in all_strikes:
        cost = 0.0
        for K, w in strikes.items():
            # intrinsic value at settlement S:
            call_iv = max(S - K, 0.0) * w["C"]
            put_iv  = max(K - S, 0.0) * w["P"]
            cost += call_iv + put_iv
        if cost < best_cost:
            best_cost, best_strike = cost, S
    return {
        "token": currency,
        "venue": venue,
        "expiration": expiration.isoformat(),
        "max_pain": best_strike,
        "total_oi_usd": None,  # optional, fill if you collect notional
        "iv_index": None
    }
