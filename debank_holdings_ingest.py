import time
import requests

def fetch_debank_holdings(wallet, retries=5):
    url = "https://api.debank.com/token/cache_balance_list"
    params = {"user_addr": wallet}

    for attempt in range(retries):
        resp = requests.get(url, params=params)
        if resp.status_code == 200:
            return resp.json().get("data", [])
        elif resp.status_code == 429:  # Too many requests
            wait = (2 ** attempt) * 5  # exponential backoff: 5s, 10s, 20s, ...
            print(f"⚠️ Rate limited. Waiting {wait}s before retry...")
            time.sleep(wait)
        else:
            resp.raise_for_status()

    raise RuntimeError(f"❌ Failed after {retries} retries (last code {resp.status_code})")
