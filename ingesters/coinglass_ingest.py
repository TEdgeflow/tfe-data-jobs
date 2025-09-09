import os
import requests
from datetime import datetime, timezone
from supabase import create_client, Client

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
COINGLASS_API_KEY = os.getenv("COINGLASS_API_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE_URL = "https://open-api-v4.coinglass.com/api"
HEADERS = {"accept": "application/json", "coinglassSecret": COINGLASS_API_KEY}


def fetch(endpoint, params=None):
    """Helper for GET requests to CoinGlass"""
    url = f"{BASE_URL}/{endpoint}"
    r = requests.get(url, headers=HEADERS, params=params or {})
    r.raise_for_status()
    return r.json()


def ingest_all():
    now = datetime.now(timezone.utc).isoformat()

    # === Get symbols from Supabase ===
    symbols = [row["symbol"] for row in sb.table("supported_symbols").select("symbol").execute().data]
    if not symbols:
        print("No symbols found in supported_symbols table.")
        return

    funding_rows, oi_rows, liq_rows, lq_rows = [], [], [], []

    for sym in symbols:
        try:
            # --- Funding Rate (exchange-list, take "All") ---
            try:
                fr_data = fetch("futures/fundingRate/exchange-list", {"symbol": sym})
                for row in fr_data.get("data", []):
                    if row.get("exchangeName") == "All":
                        funding_rows.append({
                            "symbol": sym,
                            "funding_rate": row.get("fundingRate"),
                            "timestamp": now
                        })
            except Exception as e:
                print(f"[WARN] Funding fetch failed for {sym}: {e}")

            # --- Open Interest (exchange-list, take "All") ---
            try:
                oi_data = fetch("futures/open-interest/exchange-list", {"symbol": sym})
                for row in oi_data.get("data", []):
                    if row.get("exchangeName") == "All":
                        oi_rows.append({
                            "symbol": sym,
                            "oi": row.get("openInterest"),
                            "timestamp": now
                        })
            except Exception as e:
                print(f"[WARN] OI fetch failed for {sym}: {e}")

            # --- Liquidations ---
            try:
                liq_data = fetch("futures/liquidation", {"symbol": sym})
                for l in liq_data.get("data", []):
                    liq_rows.append({
                        "symbol": sym,
                        "side": l.get("side"),
                        "amount": l.get("amount"),
                        "price": l.get("price"),
                        "time_interval": l.get("interval"),
                        "ts": now
                    })
            except Exception as e:
                print(f"[WARN] Liquidation fetch failed for {sym}: {e}")

            # --- Liquidity Levels (may not be supported in Hobbyist) ---
            try:
                lq_data = fetch("futures/liquidity", {"symbol": sym})
                for d in lq_data.get("data", []):
                    lq_rows.append({
                        "symbol": sym,
                        "bid_liquidity": d.get("bidLiquidity"),
                        "ask_liquidity": d.get("askLiquidity"),
                        "ts": now
                    })
            except Exception:
                print(f"[INFO] Liquidity not available for {sym}, skipping.")

        except Exception as e:
            print(f"[ERROR] Fatal error for {sym}: {e}")

    # === Insert into Supabase ===
    if funding_rows:
        sb.table("derivatives_funding").insert(funding_rows).execute()
        print(f"Inserted {len(funding_rows)} funding rows")

    if oi_rows:
        sb.table("derivatives_oi").insert(oi_rows).execute()
        print(f"Inserted {len(oi_rows)} OI rows")

    if liq_rows:
        sb.table("derivatives_liquidations").insert(liq_rows).execute()
        print(f"Inserted {len(liq_rows)} liquidation rows")

    if lq_rows:
        sb.table("derivatives_liquidity_levels").insert(lq_rows).execute()
        print(f"Inserted {len(lq_rows)} liquidity rows")


if __name__ == "__main__":
    ingest_all()
