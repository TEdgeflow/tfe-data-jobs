import os
import time
import requests
from datetime import datetime, timezone
from supabase import create_client, Client

# ========= ENV VARS (Railway) =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ========= CONFIG =========
COINGECKO_API = "https://api.coingecko.com/api/v3/coins/markets"
SYMBOLS = "bitcoin,ethereum,binancecoin,ripple,solana,cardano,polkadot,matic-network,tron,litecoin,okb,near,aptos,stellar,monero,uniswap,chainlink,cosmos,arbitrum,internet-computer"

# ========= FETCH =========
def fetch_coingecko_data():
    params = {"vs_currency": "usd", "ids": SYMBOLS}
    tries = 0
    while tries < 3:  # Retry up to 3 times
        try:
            resp = requests.get(COINGECKO_API, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            if resp.status_code == 429:  # Too Many Requests
                wait_time = 30 * (tries + 1)  # backoff: 30s, 60s, 90s
                print(f"Rate limit hit. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                tries += 1
            else:
                print("HTTP Error:", e)
                return []
        except Exception as e:
            print("Error fetching from CoinGecko:", e)
            return []
    print("Failed after 3 retries due to rate limits.")
    return []

# ========= UPSERT =========
def upsert_market_data(data):
    rows = []
    ts = datetime.now(timezone.utc).isoformat()

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
        print(f"[upsert] {len(rows)} Coingecko market rows…")
    else:
        print("⚠️ No rows to upsert.")

# ========= MAIN LOOP =========
def main():
    while True:
        print("Starting Coingecko Market Data Job…")
        try:
            data = fetch_coingecko_data()
            if data:
                upsert_market_data(data)
                print("✅ Done.")
            else:
                print("⚠️ No data returned from CoinGecko.")
        except Exception as e:
            print("Error during Coingecko job:", e)

        # Wait **10 minutes** before next run
        time.sleep(600)

if __name__ == "__main__":
    main()
