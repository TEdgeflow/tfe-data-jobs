import os
import json
import asyncio
import websockets
import requests
from datetime import datetime, timezone
from supabase import create_client, Client

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# 🔹 Dynamically load all USDT pairs
res = requests.get("https://api.binance.com/api/v3/exchangeInfo")
ALL_SYMBOLS = [s["symbol"].lower() for s in res.json()["symbols"] if s["quoteAsset"] == "USDT"]
print(f"✅ Loaded {len(ALL_SYMBOLS)} USDT pairs")

# 🔹 Split into shards of 50 symbols each
SHARD_SIZE = 50
SHARDS = [ALL_SYMBOLS[i:i+SHARD_SIZE] for i in range(0, len(ALL_SYMBOLS), SHARD_SIZE)]

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


async def stream_orderbook(shard_id, symbols):
    stream_url = "wss://fstream.binance.com/stream?streams=" + "/".join(
        [f"{s}@depth10@500ms" for s in symbols]
    )

    backoff = 5  # start retry delay in seconds
    while True:
        try:
            async with websockets.connect(
                stream_url,
                ping_interval=60,
                ping_timeout=20,
                close_timeout=10,
                max_queue=500,
            ) as ws:
                print(f"✅ Connected shard {shard_id} with {len(symbols)} symbols")

                backoff = 5  # reset backoff after successful connect
                while True:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=60)
                        data = json.loads(msg)
                        stream = data["stream"]
                        symbol = stream.split("@")[0]
                        await handle_message(symbol, data["data"])
                    except asyncio.TimeoutError:
                        print(f"⚠️ Shard {shard_id}: no data for 60s, sending ping...")
                        await ws.ping()
                    except websockets.ConnectionClosed:
                        print(f"⚠️ Shard {shard_id}: connection closed, breaking to reconnect...")
                        break

        except Exception as e:
            print(f"⚠️ Shard {shard_id} websocket error: {e}, retrying in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)  # exponential backoff up to 60s



async def scheduler():
    while True:
        await save_batch()
        await asyncio.sleep(BATCH_INTERVAL)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    # 🔹 Create one WebSocket task per shard
    for i, shard_symbols in enumerate(SHARDS):
        loop.create_task(stream_orderbook(i, shard_symbols))
    loop.create_task(scheduler())
    loop.run_forever()


