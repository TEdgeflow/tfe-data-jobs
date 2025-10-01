import os
import json
import asyncio
import websockets
from supabase import create_client, Client
from datetime import datetime, timezone
from websockets.exceptions import ConnectionClosedError, ConnectionClosed

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Binance liquidation stream (all symbols)
BINANCE_WS_URL = "wss://fstream.binance.com/ws/!forceOrder@arr"

async def save_liquidation(data):
    order = data["o"]
    ts = datetime.fromtimestamp(order["T"]/1000, tz=timezone.utc).isoformat()

    row = {
        "symbol": order["s"],
        "side": order["S"],  # BUY or SELL
        "price": float(order["p"]),
        "quantity": float(order["q"]),
        "time": ts
    }

    sb.table("binance_liquidations").insert(row).execute()
    print(f"[liquidation] {row['symbol']} {row['side']} {row['price']} x {row['quantity']}")

async def listen():
    """Keeps connection alive, reconnects if Binance closes it"""
    while True:
        try:
            async with websockets.connect(
                BINANCE_WS_URL,
                ping_interval=20,   # send ping every 20s
                ping_timeout=20,    # wait max 20s for pong
                close_timeout=10    # close gracefully
            ) as ws:
                print("✅ Connected to Binance Liquidations")
                while True:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=45)
                        data = json.loads(msg)
                        await save_liquidation(data)
                    except asyncio.TimeoutError:
                        # send ping manually if no msgs
                        print("⚠️ No message in 45s, sending ping")
                        await ws.ping()
        except (ConnectionClosed, ConnectionClosedError) as e:
            print(f"⚠️ Connection lost: {e} → reconnecting in 5s")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"⚠️ Unexpected error: {e} → reconnecting in 10s")
            await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(listen())

