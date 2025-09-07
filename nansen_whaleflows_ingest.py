import os
import requests
from datetime import datetime, timezone
from supabase import create_client, Client

# ========= ENV VARS (Railway) =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
NANSEN_API_KEY = os.getenv("NANSEN_API_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

HEADERS = {"Accept": "application/json", "X-API-KEY": NANSEN_API_KEY}

# ========= Endpoints =========
INFLOWS_URL = "https://api.nansen.ai/api/beta/smart-money/inflows"
OUTFLOWS_URL = "https://api.nansen.ai/api/beta/smart-money/outflows"

def fetch_nansen_data(url):
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    return resp.json()

def ingest_whale_flows():
    inflows_data = fetch_nansen_data(INFLOWS_URL)
    outflows_data = fetch_nansen_data(OUTFLOWS_URL)

    inflow_map = {}
    for item in inflows_data.get("items", []):
        token = item.get("symbol")
        chain = item.get("chain")
        inflow_usd = item.get("amount_usd", 0)
        inflow_map[(token, chain)] = inflow_usd

    outflow_map = {}
    for item in outflows_data.get("items", []):
        token = item.get("symbol")
        chain = item.get("chain")
        outflow_usd = item.get("amount_usd", 0)
        outflow_map[(token, chain)] = outflow_usd

    rows = []
    ts = datetime.now(timezone.utc).isoformat()

    all_tokens = set(inflow_map.keys()) | set(outflow_map.keys())
    for token, chain in all_tokens:
        inflow = inflow_map.get((token, chain), 0)
        outflow = outflow_map.get((token, chain), 0)
        netflow = inflow - outflow

        row = {
            "ts": ts,
            "token": token,
            "chain": chain,
            "inflow_usd": inflow,
            "outflow_usd": outflow,
            "netflow_usd": netflow,
            "sm_category": None,  # placeholder, can map later if needed
        }
        rows.append(row)

    if rows:
        sb.table("nansen_whaleflows").insert(rows).execute()
        print(f"Inserted {len(rows)} rows into nansen_whaleflows")

if __name__ == "__main__":
    ingest_whale_flows()


