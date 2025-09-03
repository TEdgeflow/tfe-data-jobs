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

STREAM_URL = "wss://fstream.binance.com/stream?streams=" + "/".join(
    [f"{s}@depth20@100ms" for s in SYMBOLS]
)

BUFFER = []
BATCH_INTERVAL = 1.0  # seconds


async def save_batch():
    global BUFFER
    if BUFFER:
        try:
            sb.table("binance_orderbook").insert(BUFFER).execute()
            print(f"✅ Inserted {len(BUFFER)} rows")
        except Exception as e:
            print(f"❌ Insert failed: {e}")
        BUFFER = []


async def handle_message(symbol, data):
    global BUFFER
    bids = data.get("bids", [])
    asks = data.get("asks", [])
    ts = datetime.fromtimestamp(data["E"] / 1000, tz=timezone.utc).isoformat()

    rows = []
    for i, (price, qty) in enumerate(bids[:10]):  # top 10 bids
        rows.append({
            "symbol": symbol.upper(),
            "side": "BID",
            "price": float(price),
            "quantity": float(qty),
            "depth_level": i + 1,
            "time": ts
        })
    for i, (price, qty) in enumerate(asks[:10]):  # top 10 asks
        rows.append({
            "symbol": symbol.upper(),
            "side": "ASK",
            "price": float(price),
            "quantity": float(qty),
            "depth_level": i + 1,
            "time": ts
        })

    BUFFER.extend(rows)


async def stream_orderbook():
    while True:  # auto-reconnect loop
        try:
            async with websockets.connect(
                STREAM_URL,
                ping_interval=20,   # keepalive every 20s
                ping_timeout=20
            ) as ws:
                print("✅ Connected to Binance Multi-Symbol Order Book")

                while True:
                    msg = await ws.recv()
                    data = json.loads(msg)
                    stream = data["stream"]
                    symbol = stream.split("@")[0]
                    await handle_message(symbol, data["data"])

        except Exception as e:
            print(f"⚠️ Websocket error: {e}, retrying in 5s...")
            await asyncio.sleep(5)  # wait before reconnect


async def scheduler():
    while True:
        await save_batch()
        await asyncio.sleep(BATCH_INTERVAL)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(stream_orderbook())
    loop.create_task(scheduler())
    loop.run_forever()
