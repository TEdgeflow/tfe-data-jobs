import os
import json
import asyncio
import websockets
import requests
import time
from datetime import datetime, timezone, timedelta
from supabase import create_client, Client

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ========= LOAD SYMBOLS =========
res = requests.get("https://api.binance.com/api/v3/exchangeInfo")
ALL_SYMBOLS = [s["symbol"].lower() for s in res.json()["symbols"] if s["quoteAsset"] == "USDT"]
print(f"✅ Loaded {len(ALL_SYMBOLS)} USDT pairs")

# ========= SPLIT INTO SHARDS =========
SHARD_SIZE = 50
SHARDS = [ALL_SYMBOLS[i:i + SHARD_SIZE] for i in range(0, len(ALL_SYMBOLS), SHARD_SIZE)]

BUFFER = []
BATCH_INTERVAL = 1.0  # seconds
rows_written = 0
_last_stats = time.time()
_last_debug = {}

# ========= SAVE BATCH =========
async def save_batch():
    global BUFFER, rows_written, _last_stats
    if BUFFER:
        try:
            sb.table("binance_orderbook").insert(BUFFER).execute()
            rows_written += len(BUFFER)
            print(f"✅ Inserted {len(BUFFER)} rows")
        except Exception as e:
            print(f"❌ Insert failed: {e}")
            print(f"Example row: {BUFFER[0] if BUFFER else 'EMPTY'}")
        BUFFER = []

    # 🧠 Health print every 60s
    now = time.time()
    if now - _last_stats > 60:
        print(f"🩵 Health check → {rows_written:,} rows inserted in last 60s")
        rows_written = 0
        _last_stats = now

# ========= HANDLE MESSAGE =========
async def handle_message(symbol, data):
    global BUFFER, _last_debug

    bids = data.get("bids", []) or data.get("data", {}).get("bids", [])
    asks = data.get("asks", []) or data.get("data", {}).get("asks", [])
    ts = datetime.fromtimestamp(data.get("E", data.get("data", {}).get("E", 0)) / 1000, tz=timezone.utc).isoformat()

    # 🧠 Debug every 5s per symbol
    now = time.time()
    if symbol not in _last_debug or now - _last_debug[symbol] > 5:
        print(f"DEBUG {symbol.upper()} → bids={len(bids)}, asks={len(asks)}")
        _last_debug[symbol] = now

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
        BUFFER.extend(rows)

# ========= STREAM ORDERBOOK =========
async def stream_orderbook(shard_id, symbols):
    stream_url = "wss://fstream.binance.com/stream?streams=" + "/".join(
        [f"{s}@depth10@500ms" for s in symbols]
    )
    backoff = 5
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
                backoff = 5
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
                        print(f"⚠️ Shard {shard_id}: connection closed, reconnecting...")
                        break
        except Exception as e:
            print(f"⚠️ Shard {shard_id} websocket error: {e}, retrying in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

# ========= SCHEDULER =========
async def scheduler():
    while True:
        await save_batch()
        await asyncio.sleep(BATCH_INTERVAL)

# ========= WATCHDOG =========
async def watchdog():
    while True:
        try:
            result = sb.table("binance_orderbook") \
                .select("time") \
                .order("time", desc=True) \
                .limit(1) \
                .execute()
            if result.data:
                latest = result.data[0]["time"]
                print(f"🕐 Watchdog: latest insert at {latest}")
            else:
                print("⚠️ Watchdog: no data found")
        except Exception as e:
            print(f"⚠️ Watchdog error: {e}")
        await asyncio.sleep(600)  # every 10 minutes

# ========= AUTO CLEANUP =========
async def cleanup_old_rows():
    """Deletes rows older than 7 days from binance_orderbook daily."""
    while True:
        try:
            cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
            print(f"🧹 Cleanup: deleting rows older than {cutoff}")
            sb.table("binance_orderbook").delete().lt("time", cutoff).execute()
            print(f"✅ Cleanup completed at {datetime.now(timezone.utc).isoformat()}")
        except Exception as e:
            print(f"⚠️ Cleanup failed: {e}")
        await asyncio.sleep(86400)  # run once per day

# ========= MAIN =========
if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    for i, shard_symbols in enumerate(SHARDS):
        loop.create_task(stream_orderbook(i + 1, shard_symbols))
    loop.create_task(scheduler())
    loop.create_task(watchdog())
    loop.create_task(cleanup_old_rows())  # 👈 Auto-cleanup added

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        print("🛑 Shutting down gracefully...")
    except Exception as e:
        print(f"❌ Unhandled error in main loop: {e}")
    finally:
        for task in asyncio.all_tasks(loop):
            task.cancel()
        loop.stop()
        loop.close()


