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

# Symbols we want to track
SYMBOLS = ["btcusdt", "ethusdt", "bnbusdt", "solusdt"]

# Build multi-stream URL
STREAM_URL = "wss://fstream.binance.com/stream?streams=" + "/".join(
    [f"{s}@depth20@100ms" for s in SYMBOLS]
)

async def save_orderbook(symbol, data):
    # ✅ Binance fields: "b" = bids, "a" = asks
    bids = data.get("b", [])
    asks = data.get("a", [])
    ts = datetime.fromtimestamp(data["E"] / 1000, tz=timezone.utc).isoformat()

    rows = []

    # Insert bids
    for i, (price, qty) in enumerate(bids):
        rows.append({
            "symbol": symbol.upper(),
            "side": "BID",
            "price": float(price),
            "quantity": float(qty),
            "depth_level": i + 1,
            "time": ts
        })

    # Insert asks
    for i, (price, qty) in enumerate(asks):
        rows.append({
            "symbol": symbol.upper(),
            "side": "ASK",
            "price": float(price),
            "quantity": float(qty),
            "depth_level": i + 1,
            "time": ts
        })

    if rows:
        sb.table("binance_orderbook").insert(rows).execute()
        print(f"[orderbook] {symbol.upper()} {len(rows)} rows ✅ at {ts}")
    else:
        print(f"⚠️ No rows to insert for {symbol.upper()} at {ts}")

async def main():
    async with websockets.connect(STREAM_URL) as ws:
        print("✅ Connected to Binance Multi-Symbol Order Book")
        while True:
            msg = await ws.recv()
            data = json.loads(msg)

            stream = data["stream"]      # e.g. "btcusdt@depth20@100ms"
            symbol = stream.split("@")[0]

            await save_orderbook(symbol, data["data"])

if __name__ == "__main__":
    asyncio.run(main())
