# binance_liquidations_ingest.py
import os, json, asyncio, random, signal
import websockets
from datetime import datetime, timezone
from supabase import create_client, Client
from websockets.exceptions import ConnectionClosed, ConnectionClosedError

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Futures liquidations aggregated stream (arr = array, all symbols)
# For SPOT use: wss://stream.binance.com:9443/stream?streams=!forceOrder@arr
WS_URL = "wss://fstream.binance.com/stream?streams=!forceOrder@arr"

BATCH_SEC = 1.0
BUFFER_MAX = 5000
RECV_TIMEOUT = 45         # seconds without messages => we ping
PING_INTERVAL = 20        # seconds between automatic pings
PING_TIMEOUT  = 20        # seconds to wait for pong
CLOSE_TIMEOUT = 10
MAX_QUEUE     = 2000
MAX_SIZE      = 2 ** 20   # 1 MB

buffer = []
stop_event = asyncio.Event()

async def save_loop():
    """Periodic batch flush to Supabase."""
    global buffer
    while not stop_event.is_set():
        try:
            if buffer:
                to_insert, buffer = buffer, []
                sb.table("binance_liquidations").insert(to_insert).execute()
                print(f"[liquidations] inserted {len(to_insert)} rows")
        except Exception as e:
            print(f"[liquidations] insert failed: {e}")
        await asyncio.sleep(BATCH_SEC)

def parse_force_order(msg: dict):
    """
    Binance futures force order payload shape:
    {
      "e":"forceOrder","E":..., "o":{"s":"BTCUSDT","S":"SELL","o":"LIMIT",...,"p":"29100.00","q":"0.01","ap":"29105.0","X":"FILLED",...}
    }
    """
    try:
        o = msg["o"]
        ts = datetime.fromtimestamp(msg["E"]/1000, tz=timezone.utc).isoformat()
        return {
            "symbol": o["s"],
            "side":   o["S"],
            "order_type": o.get("o"),
            "status": o.get("X"),
            "price": float(o.get("p") or 0),
            "avg_price": float(o.get("ap") or 0),
            "qty":   float(o.get("q") or 0),
            "time":  ts
        }
    except Exception:
        return None

async def stream_loop():
    """Connect, read messages, and reconnect on errors with backoff."""
    backoff = 1
    while not stop_event.is_set():
        try:
            print("[liquidations] connecting websocket...")
            async with websockets.connect(
                WS_URL,
                ping_interval=PING_INTERVAL,
                ping_timeout=PING_TIMEOUT,
                close_timeout=CLOSE_TIMEOUT,
                max_queue=MAX_QUEUE,
                max_size=MAX_SIZE,
            ) as ws:
                print("[liquidations] connected")

                # reset backoff after successful connect
                backoff = 1

                while not stop_event.is_set():
                    try:
                        # Protect recv with timeout so we can ping proactively
                        raw = await asyncio.wait_for(ws.recv(), timeout=RECV_TIMEOUT)
                        data = json.loads(raw)
                        # multiplexer payload -> { "stream": "...", "data": {...} }
                        payload = data.get("data") or data
                        if payload.get("e") == "forceOrder":
                            row = parse_force_order(payload)
                            if row:
                                buffer.append(row)
                                if len(buffer) >= BUFFER_MAX:
                                    # small yield so save_loop can run
                                    await asyncio.sleep(0)
                        else:
                            # ignore non-forceOrder events
                            pass

                    except asyncio.TimeoutError:
                        # We didn’t get any message; send a ping to keep the connection alive
                        print("[liquidations] no messages; sending ping")
                        try:
                            await ws.ping()
                        except Exception as e:
                            print(f"[liquidations] ping failed: {e}")
                            raise

        except (ConnectionClosed, ConnectionClosedError) as e:
            # typical: 1006/1011 keepalive timeouts; backoff and reconnect
            print(f"[liquidations] websocket closed: {e}; reconnecting in {backoff}s")
            await asyncio.sleep(backoff + random.random())
            backoff = min(backoff * 2, 60)
        except Exception as e:
            print(f"[liquidations] unexpected error: {e}; reconnecting in {backoff}s")
            await asyncio.sleep(backoff + random.random())
            backoff = min(backoff * 2, 60)

async def main():
    # graceful shutdown on SIGTERM/SIGINT
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    saver = asyncio.create_task(save_loop())
    streamer = asyncio.create_task(stream_loop())
    try:
        await asyncio.gather(streamer)
    finally:
        stop_event.set()
        await asyncio.sleep(0.1)
        # final flush
        if buffer:
            try:
                sb.table("binance_liquidations").insert(buffer).execute()
                print(f"[liquidations] final flush {len(buffer)} rows")
            except Exception as e:
                print(f"[liquidations] final flush failed: {e}")
        saver.cancel()
        with contextlib.suppress(Exception):
            await saver

if __name__ == "__main__":
    asyncio.run(main())


