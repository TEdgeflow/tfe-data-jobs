import os
import time
import requests
from datetime import datetime, timezone
from supabase import create_client, Client

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
COINGLASS_API_KEY = os.getenv("COINGLASS_API_KEY")

if not SUPABASE_URL or not SUPABASE_KEY or not COINGLASS_API_KEY:
    raise RuntimeError("Missing SUPABASE_URL, SUPABASE_KEY, or COINGLASS_API_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE_URL = "https://open-api-v4.coinglass.com/api"
HEADERS = {"accept": "application/json", "CG-API-KEY": COINGLASS_API_KEY}

# ========= CONFIG =========
INTERVALS = ["4h", "1d"]   # All supported coins
EXTRA_INTERVAL = "1h"      # Top 50 only
TOP_N = 50                 # number of top coins for 1h pull

# ========= HELPERS =========
def fetch(endpoint, params=None):
    url = f"{BASE_URL}{endpoint}"
    try:
        r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        if r.status_code == 200:
            return r.json()
        else:
            print(f"[warn] {url} -> {r.status_code} {r.text}")
            return None
    except Exception as e:
        print(f"[error] {url} -> {e}")
        return None


def insert_rows(table, rows):
    if rows:
        try:
            sb.table(table).insert(rows).execute()
            print(f"[ok] inserted {len(rows)} rows into {table}")
        except Exception as e:
            print(f"[error] insert {table} -> {e}")


# ========= MAIN INGEST =========
def run():
    # --- Step 1: Get supported futures coins
    data = fetch("/futures/supported-coins")
    if not data or "data" not in data:
        print("[error] failed to fetch supported coins")
        return
    coins = [c["symbol"] for c in data["data"]]
    print(f"[info] total supported coins = {len(coins)}")

    # --- Step 2: Pick top N coins by OI (for 1h interval)
    oi_data = fetch("/futures/openInterest/oi-coin-list")
    top_coins = []
    if oi_data and "data" in oi_data:
        sorted_list = sorted(
            oi_data["data"], key=lambda x: float(x.get("openInterest", 0)), reverse=True
        )
        top_coins = [c["symbol"] for c in sorted_list[:TOP_N]]
        print(f"[info] top {TOP_N} coins for 1h = {top_coins[:10]}...")

    # --- Step 3: Loop coins and fetch metrics
    for sym in coins:
        for interval in INTERVALS + ([EXTRA_INTERVAL] if sym in top_coins else []):
            # OI
            oi = fetch(
                "/futures/openInterest/ohlc-aggregated-history",
                {"symbol": sym, "interval": interval},
            )
            if oi and "data" in oi:
                rows = [
                    {
                        "symbol": sym,
                        "oi": float(item.get("openInterest", 0)),
                        "interval": interval,
                        "timestamp": datetime.fromtimestamp(
                            item["time"] / 1000, tz=timezone.utc
                        ).isoformat(),
                    }
                    for item in oi["data"]
                ]
                insert_rows("derivatives_oi", rows)

            # Funding
            fr = fetch(
                "/futures/fundingRate/oi-weight-ohlc-history",
                {"symbol": sym, "interval": interval},
            )
            if fr and "data" in fr:
                rows = [
                    {
                        "symbol": sym,
                        "funding_rate": float(item.get("fundingRate", 0)),
                        "interval": interval,
                        "timestamp": datetime.fromtimestamp(
                            item["time"] / 1000, tz=timezone.utc
                        ).isoformat(),
                    }
                    for item in fr["data"]
                ]
                insert_rows("derivatives_funding", rows)

            # Liquidations
            liq = fetch(
                "/futures/liquidation/aggregated-history",
                {"symbol": sym, "interval": interval},
            )
            if liq and "data" in liq:
                rows = [
                    {
                        "symbol": sym,
                        "amount": float(item.get("amount", 0)),
                        "price": float(item.get("price", 0)),
                        "side": item.get("side", "na"),
                        "interval": interval,
                        "timestamp": datetime.fromtimestamp(
                            item["time"] / 1000, tz=timezone.utc
                        ).isoformat(),
                    }
                    for item in liq["data"]
                ]
                insert_rows("derivatives_liquidations", rows)

            # Taker Buy/Sell Volume
            tv = fetch(
                "/futures/taker-buy-sell-volume/history",
                {"symbol": sym, "interval": interval},
            )
            if tv and "data" in tv:
                rows = [
                    {
                        "symbol": sym,
                        "buy_volume": float(item.get("buyVolUsd", 0)),
                        "sell_volume": float(item.get("sellVolUsd", 0)),
                        "interval": interval,
                        "timestamp": datetime.fromtimestamp(
                            item["time"] / 1000, tz=timezone.utc
                        ).isoformat(),
                    }
                    for item in tv["data"]
                ]
                insert_rows("derivatives_taker_volume", rows)


if __name__ == "__main__":
    while True:
        run()
        print("[sleep] waiting 15 min...")
        time.sleep(900)














