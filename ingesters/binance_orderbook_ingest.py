import os
import json
import time
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
SHARDS = [ALL_SYMBOLS[i:i + SHARD_SIZE] for i in range(0, len(ALL_SYMBOLS), SHARD_SIZE)]

# Globals
BUFFER = []
BATCH_INTERVAL = 1.0  # seconds
rows_written = 0
_last_stats = time.time()
_last_debug = {}  # per-symbol debug timing


# ==========================================================
# 🔹 Batch Writer
# ==========================================================
async def save_batch():
    """Insert buffered rows into Supabase table periodically."""
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

    # 🧠 Health print every 60 seconds
    now = time.time()
    if now - _last_stats > 60:
        print(f"🩵 Health check → {rows_written:,} rows inserted in last 60s")
        rows_written = 0
        _last_stats = now


# ==========================================================
# 🔹 Handle WebSocket Payloads
# ==========================================================
import time
_last_debug = {}

async def handle_message(symbol, data):
    global BUFFER, _last_debug

    # Handle both 'data' and nested 'data.data' cases
    if "data" in data:
        data = data["data"]

    # Sometimes Binance sends empty partial update → ignore if no bids/asks
    bids = data.get("bids") or data.get("b") or []
    asks = data.get("asks") or data.get("a") or []

    # Extract timestamp safely
    ts_val = data.get("E") or data.get("T") or time.time() * 1000
    ts = datetime.fromtimestamp(ts_val / 1000, tz=timezone.utc).isoformat()

    # Log every 10s per symbol, only if bids/asks missing or new pattern
    now = time.time()
    if symbol not in _last_debug or now - _last_debug[symbol] > 10:
        print(
            f"DEBUG {symbol.upper()} → keys={list(data.keys())}, "
            f"bids={len(bids)}, asks={len(asks)}, event={data.get('e')}"
        )
        _last_debug[symbol] = now

    # If both sides empty, skip
    if not bids and not asks:
        return

    # Build top-10 rows per side
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

    BUFFER.extend(rows)

# ==========================================================
# 🔹 WebSocket Stream (Fixed Payload)
# ==========================================================
async def stream_orderbook(shard_id, symbols):
    """Stream orderbook updates from Binance and feed them into the buffer."""
    stream_url = "wss://fstream.binance.com/stream?streams=" + "/".join(
        [f"{s}@depth10@500ms" for s in symbols]
    )

    backoff = 5  # start retry delay
    while True:
        try:
            async with websockets.connect(
                stream_url,
                ping_interval=15,
                ping_timeout=15,
                close_timeout=10,
                max_queue=500,
            ) as ws:
                print(f"✅ Connected shard {shard_id} with {len(symbols)} symbols")
                backoff = 5  # reset on successful connect

                while True:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=30)
                        data = json.loads(msg)

                        # Binance sends: {"stream": "btcusdt@depth10@500ms", "data": {...}}
                        payload = data.get("data", {})
                        if not payload:
                            continue

                        stream = data.get("stream", "")
                        symbol = stream.split("@")[0]
                        await handle_message(symbol, payload)

                    except asyncio.TimeoutError:
                        print(f"⚠️ Shard {shard_id}: no data for 30s, sending ping...")
                        await ws.ping()
                    except websockets.ConnectionClosed:
                        print(f"⚠️ Shard {shard_id}: connection closed, reconnecting...")
                        break
                    except Exception as e:
                        print(f"⚠️ Shard {shard_id}: internal error: {e}")
                        break

        except Exception as e:
            print(f"⚠️ Shard {shard_id} websocket error: {e}, retrying in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


# ==========================================================
# 🔹 Periodic Scheduler
# ==========================================================
async def scheduler():
    """Runs save_batch() every BATCH_INTERVAL seconds."""
    while True:
        await save_batch()
        await asyncio.sleep(BATCH_INTERVAL)


# ==========================================================
# 🔹 Watchdog: Sanity Check
# ==========================================================
async def watchdog():
    """Checks that new data keeps arriving in Supabase."""
    while True:
        try:
            result = sb.table("binance_orderbook").select("time").order("time", desc=True).limit(1).execute()
            if result.data and len(result.data) > 0:
                latest = result.data[0]["time"]
                print(f"🕐 Watchdog check: latest insert at {latest}")
            else:
                print("⚠️ Watchdog warning: no data returned from binance_orderbook")
        except Exception as e:
            print(f"⚠️ Watchdog error: {e}")
        await asyncio.sleep(600)  # every 10 minutes


# ===============================
# 🧹 SIMPLE DAILY CLEANUP TASK
# ===============================
import asyncio
from datetime import datetime, timezone, timedelta

async def cleanup_old_rows():
    """Deletes rows older than 7 days from binance_orderbook daily."""
    while True:
        try:
            cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
            print(f"🧹 Running cleanup: deleting rows older than {cutoff}")

            sb.table("binance_orderbook").delete().lt("time", cutoff).execute()

            print(f"✅ Cleanup completed at {datetime.now(timezone.utc).isoformat()}")
        except Exception as e:
            print(f"⚠️ Cleanup failed: {e}")

        # Sleep for 24 hours before next cleanup
        await asyncio.sleep(86400)  # 86400 seconds = 1 day


# ==========================================================
# 🔹 Entry Point
# ==========================================================
if __name__ == "__main__":
    loop = asyncio.get_event_loop()

    # Create shard tasks
    for i, shard_symbols in enumerate(SHARDS):
        loop.create_task(stream_orderbook(i + 1, shard_symbols))

    loop.create_task(scheduler())
    loop.create_task(watchdog())
    loop.create_task(cleanup_old_rows())


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




