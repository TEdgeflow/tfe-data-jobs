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

STREAM_URL = "wss://fstream.binance.com/stream?streams=" + "/".join(
    [f"{s}@depth10@500ms" for s in SYMBOLS]
)

BUFFER = []
BATCH_INTERVAL = 1.0  # seconds


async def save_batch():
    global BUFFER
    if BUFFER:
        try:
            sb.table("binance_orderbook") \
              .upsert(
                  BUFFER,
                  on_conflict=["symbol", "side", "depth_level", "time"]
              ) \
              .execute()
            print(f"✅ Upserted {len(BUFFER)} rows (example row: {BUFFER[0]})")
        except Exception as e:
            print(f"❌ Upsert failed: {e}")
        BUFFER = []



async def handle_message(symbol, data):
    global BUFFER
    bids = data.get("bids", [])
    asks = data.get("asks", [])
    ts = datetime.fromtimestamp(data["E"] / 1000, tz=timezone.utc).isoformat()

    rows = []
    for i, (price, qty) in enumerate(bids[:10]):
        rows.append({
            "symbol": symbol.upper(),
            "side": "BID",
            "price": float(price),
            "quantity": float(qty),
            "depth_level": i + 1,
            "time": ts
        })
    for i, (price, qty) in enumerate(asks[:10]):
        rows.append({
            "symbol": symbol.upper(),
            "side": "ASK",
            "price": float(price),
            "quantity": float(qty),
            "depth_level": i + 1,
            "time": ts
        })

    if rows:
        print(f"[handle_message] {symbol.upper()} adding {len(rows)} rows, ts={ts}")
    BUFFER.extend(rows)


async def stream_orderbook():
    while True:  # auto-reconnect loop
        try:
            async with websockets.connect(
                STREAM_URL,
                ping_interval=15,
                ping_timeout=15
            ) as ws:
                print("✅ Connected to Binance Order Book stream")
                while True:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=30)
                        data = json.loads(msg)
                        stream = data["stream"]
                        symbol = stream.split("@")[0]
                        # Debug raw message for first symbol only
                        if symbol == "btcusdt":
                            print(f"[stream_orderbook] raw BTCUSDT snapshot: {str(data)[:200]}...")
                        await handle_message(symbol, data["data"])
                    except asyncio.TimeoutError:
                        print("⚠️ No message for 30s, sending ping...")
                        await ws.ping()
        except Exception as e:
            print(f"⚠️ Websocket error: {e}, retrying in 5s...")
            await asyncio.sleep(5)


async def scheduler():
    while True:
        await save_batch()
        await asyncio.sleep(BATCH_INTERVAL)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(stream_orderbook())
    loop.create_task(scheduler())
    loop.run_forever()

