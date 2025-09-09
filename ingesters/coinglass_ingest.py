import os
import requests

# ========= ENV VARS =========
COINGLASS_KEY = os.getenv("COINGLASS_KEY")

if not COINGLASS_KEY:
    raise RuntimeError("Missing COINGLASS_KEY")

HEADERS = {
    "CG-API-KEY": COINGLASS_KEY,
    "accept": "application/json"
}
BASE_URL = "https://open-api-v4.coinglass.com/api"

def test_supported_coins():
    url = f"{BASE_URL}/futures/supported-coins"
    r = requests.get(url, headers=HEADERS)
    print("Status:", r.status_code)
    print("Raw Response:", r.text[:500])  # first 500 chars in case it's long

    if r.status_code != 200:
        return
    
    data = r.json()
    coins = data.get("data", [])
    print("Total coins returned:", len(coins))
    print("First 5 symbols:", [c.get("symbol") for c in coins[:5]])

if __name__ == "__main__":
    test_supported_coins()










