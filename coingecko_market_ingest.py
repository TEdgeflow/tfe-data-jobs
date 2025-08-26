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

# ========= CONFIG =========
COINGECKO_API = "https://api.coingecko.com/api/v3/coins/markets"
SYMBOLS = (
    "bitcoin,ethereum,binancecoin,ripple,solana,cardano,polkadot,"
    "matic-network,tron,litecoin,okb,near,aptos,stellar,monero,"
    "uniswap,chainlink,cosmos,arbitrum,internet-computer"
)

# ========= FETCH =========
def fetch_coingecko_data():
    params = {"vs_currency": "usd", "ids": SYMBOLS}
    tries = 0
    while tries < 3:  # retry up to 3 times
        try:
            resp = requests.get(COINGECKO_API, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            if resp.status_code == 429:
                wait_time = 60 * (tries + 1)  # backoff: 1m → 2m → 3m
                print(f"⚠️ Rate limited by CoinGecko. Retrying in {wait_time}s...")
                time.sleep(wait_time)
                tries += 1
            else:
                print("HTTP error:", e)
                return []
        except Exception as e:
            print("Error fetching:", e)
            return []
    return []

# ========= UPSERT =========
def upsert_market_data(data):
    ts = datetime.now(timezone.utc).isoformat()
    rows = []
    for coin in data:
        rows.append({
            "ts": ts,
            "symbol": coin.get("symbol", "").upper() + "USDT",
            "price": coin.get("current_price"),
            "volume_24h": coin.get("total_volume"),
            "market_cap": coin.get("market_cap"),
            "price_change_24h": coin.get("price_change_percentage_24h"),
        })
    if rows:
        sb.table("market_data").upsert(rows).execute()
        print(f"[upsert] {len(rows)} Coingecko rows stored.")
    else:
        print("⚠️ No rows to upsert.")

# ========= MAIN =========
def main():
    while True:
        print("Starting Coingecko Market Data Job…")
        data = fetch_coingecko_data()
        if data:
            upsert_market_data(data)
            print("✅ Done.")
        else:
            print("⚠️ No data returned from CoinGecko.")
        time.sleep(600)  # run every 10 minutes

if __name__ == "__main__":
    main()

