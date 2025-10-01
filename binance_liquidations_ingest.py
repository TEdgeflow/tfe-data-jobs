import os
import json
import asyncio
import websockets
from supabase import create_client, Client
from datetime import datetime, timezone

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Binance liquidation stream (all symbols)
BINANCE_WS_URL = "wss://fstream.binance.com/ws/!forceOrder@arr"

# Buffer for batch inserts
BUFFER = []
BATCH_INTERVAL = 1.0  # seconds


async def save_batch():
    """Insert buffered liquidations into Supabase in batches"""
    global BUFFER
    if not BUFFER:
        return
    try:
        sb.table("binance_liquidations").insert(BUFFER).execute()
        print(f"✅ Inserted {len(BUFFER)} rows")
    except Exception as e:
        print(f"❌ Batch insert failed: {e}")
    BUFFER = []


async def handle_message(data):
    """Process a single liquidation event"""
    global BUFFER
    order = data["o"]
    ts = datetime.fromtimestamp(order["T"] / 1000, tz=timezone.utc).isoformat()

    row = {
        "symbol": order["s"],
        "side": order["S"],  # BUY or SELL
        "price": float(order["p"]),
        "quantity": float(order["q"]),
        "time": ts
    }

    BUFFER.append(row)


async def listen():
    """Listen to Binance liquidations with auto-reconnect and batching"""
    while True:
        try:
            async with websockets.connect(
                BINANCE_WS_URL,
                ping_interval=20,
                ping_timeout=20
            ) as ws:
                print("✅ Connected to Binance Liquidations")

                while True:
                    msg = await ws.recv()
                    data = json.loads(msg)
                    await handle_message(data)

        except Exception as e:
            print(f"⚠️ Websocket error: {e}, retrying in 5s...")
            await asyncio.sleep(5)


async def scheduler():
    """Periodically flush buffer to Supabase"""
    while True:
        await save_batch()
        await asyncio.sleep(BATCH_INTERVAL)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(listen())
    loop.create_task(scheduler())
    loop.run_forever()


