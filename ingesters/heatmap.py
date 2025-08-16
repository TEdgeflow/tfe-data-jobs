# ingesters/heatmap.py
import requests, math
from datetime import datetime, timezone

def iso_now():
    return datetime.now(tz=timezone.utc).isoformat()

def fetch_binance_depth(symbol: str, limit: int = 1000) -> dict:
    url = "https://fapi.binance.com/fapi/v1/depth"
    r = requests.get(url, params={"symbol": symbol, "limit": limit}, timeout=10)
    r.raise_for_status()
    return r.json()

def bucketize_depth(symbol: str, venue: str, depth: dict, bucket_bps: float = 10.0) -> list[dict]:
    """
    bucket_bps = bucket size in basis points (10 bps = 0.1%)
    We sum size*price into each bucket for bids and asks.
    """
    ts = iso_now()
    # Try to approximate mid from best quotes
    bids = depth.get("bids", [])
    asks = depth.get("asks", [])
    if not bids or not asks:
        return []
    best_bid = float(bids[0][0])
    best_ask = float(asks[0][0])
    mid = (best_bid + best_ask) / 2.0
    bucket_size = mid * (bucket_bps / 10000.0)

    out = []
    def add_side(levels, side):
        agg = {}
        for px_str, qty_str in levels:
            px = float(px_str); qty = float(qty_str)
            # bucket center
            k = math.floor(px / bucket_size) * bucket_size
            agg[k] = agg.get(k, 0.0) + (qty * px)  # USD notional
        for k, notional in agg.items():
            out.append({
                "ts": ts,
                "venue": venue,
                "symbol": symbol,
                "price_level": round(k, 2),
                "side": side,
                "liquidity_usd": notional
            })
    add_side(bids, "BID")
    add_side(asks, "ASK")
    return out
