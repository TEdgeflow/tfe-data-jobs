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

async def main():
    async with websockets.connect(BINANCE_WS_URL) as ws:
        print("✅ Connected to Binance Liquidations")
        while True:
            msg = await ws.recv()
            data = json.loads(msg)
            await save_liquidation(data)

if __name__ == "__main__":
    asyncio.run(main())

