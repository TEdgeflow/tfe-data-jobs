import os
import json
import asyncio
import websockets
from datetime import datetime, timezone
from supabase import create_client, Client

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Binance orderbook stream (top 20 levels, every 100ms)
STREAM_URL = "wss://fstream.binance.com/ws/btcusdt@depth20@100ms"

async def save_orderbook(snapshot):
    bids = snapshot.get("bids", [])
    asks = snapshot.get("asks", [])
    ts = datetime.fromtimestamp(snapshot["E"] / 1000, tz=timezone.utc).isoformat()

    rows = []

    for i, (price, qty) in enumerate(bids):
        rows.append({
            "symbol": snapshot["s"],
            "side": "BID",
            "price": float(price),
            "quantity": float(qty),
            "depth_level": i + 1,
            "time": ts
        })

    for i, (price, qty) in enumerate(asks):
        rows.append({
            "symbol": snapshot["s"],
            "side": "ASK",
            "price": float(price),
            "quantity": float(qty),
            "depth_level": i + 1,
            "time": ts
        })

    if rows:
        sb.table("binance_orderbook").insert(rows).execute()
        print(f"[orderbook] Saved {len(rows)} rows for {snapshot['s']}")

async def main():
    async with websockets.connect(STREAM_URL) as ws:
        print("✅ Connected to Binance Order Book Stream")
        while True:
            msg = await ws.recv()
            data = json.loads(msg)
            await save_orderbook(data)

if __name__ == "__main__":
    asyncio.run(main())
