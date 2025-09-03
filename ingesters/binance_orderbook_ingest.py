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

# Symbols to track
SYMBOLS = ["btcusdt", "ethusdt", "bnbusdt", "solusdt"]

# Multi-stream WebSocket URL
STREAM_URL = "wss://fstream.binance.com/stream?streams=" + "/".join(
    [f"{s}@depth20@1000ms" for s in SYMBOLS]  # 1 second updates (instead of 100ms)
)

async def save_orderbook(symbol, data):
    bids = data.get("bids", [])
    asks = data.get("asks", [])
    ts = datetime.fromtimestamp(data["E"] / 1000, tz=timezone.utc).isoformat()

    rows = []

    for i, (price, qty) in enumerate(bids):
        rows.append({
            "symbol": symbol.upper(),
            "side": "BUY",   # ✅ match DB convention
            "price": float(price),
            "quantity": float(qty),
            "depth_level": i + 1,
            "time": ts
        })

    for i, (price, qty) in enumerate(asks):
        rows.append({
            "symbol": symbol.upper(),
            "side": "SELL",  # ✅ match DB convention
            "price": float(price),
            "quantity": float(qty),
            "depth_level": i + 1,
            "time": ts
        })

    if rows:
        try:
            sb.table("binance_orderbook").insert(rows).execute()
            print(f"[orderbook] ✅ Inserted {len(rows)} rows for {symbol.upper()} at {ts}")
        except Exception as e:
            print(f"[orderbook] ❌ Error inserting rows: {e}")

async def main():
    async with websockets.connect(STREAM_URL) as ws:
        print("✅ Connected to Binance Multi-Symbol Order Book")
        while True:
            msg = await ws.recv()
            data = json.loads(msg)

            stream = data["stream"]      # e.g. "btcusdt@depth20@1000ms"
            symbol = stream.split("@")[0]

            await save_orderbook(symbol, data["data"])

if __name__ == "__main__":
    asyncio.run(main())

