import os
import time
import requests
from datetime import datetime, timezone
from supabase import create_client, Client

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ========= Fake browser headers (bypass scrape protection) =========
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/113.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.coinglass.com/"
}

# ========= Coinglass endpoints =========
# We'll loop through multiple symbols + exchanges
BASE_URL = "https://www.coinglass.com/api/pro/v1/futures/liquidationMap"

SYMBOLS = ["BTC", "ETH", "SOL", "XRP", "DOGE"]
EXCHANGES = ["Binance", "Bybit", "OKX"]


def fetch_liquidation_map(symbol: str, exchange: str):
    """Scrape liquidation heatmap for one symbol & exchange"""
    params = {"symbol": symbol, "interval": "24h", "exchangeName": exchange}
    resp = requests.get(BASE_URL, headers=HEADERS, params=params)
    if resp.status_code != 200:
        print(f"❌ {symbol}-{exchange} failed: {resp.status_code}")
        return []
    data = resp.json()
    if not data.get("data"):
        print(f"⚠️ No data for {symbol}-{exchange}")
        return []
    return data["data"]


def upsert_liquidations(symbol: str, exchange: str, data: list):
    """Insert liquidation map into Supabase"""
    rows = []
    ts = datetime.now(timezone.utc).isoformat()

    for d in data:
        rows.append({
            "ts": ts,
            "symbol": symbol,
            "exchange": exchange,
            "price": d.get("price"),
            "long_liq": d.get("longVol"),
            "short_liq": d.get("shortVol"),
        })

    if rows:
        sb.table("coinglass_liquidations").upsert(rows).execute()
        print(f"[upsert] {len(rows)} rows for {symbol}-{exchange}")


def main():
    while True:
        print("🚀 Fetching Coinglass liquidation heatmaps...")
        for symbol in SYMBOLS:
            for exchange in EXCHANGES:
                try:
                    data = fetch_liquidation_map(symbol, exchange)
                    if data:
                        upsert_liquidations(symbol, exchange, data)
                    time.sleep(2)  # ⏳ avoid rate-limit
                except Exception as e:
                    print(f"❌ Error {symbol}-{exchange}: {e}")
        print("✅ Cycle complete. Sleeping 30 minutes...\n")
        time.sleep(1800)  # run every 30 minutes


if __name__ == "__main__":
    main()
