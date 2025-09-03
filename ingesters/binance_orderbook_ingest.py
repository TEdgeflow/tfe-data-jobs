import os
import json
import asyncio
import websockets
from datetime import datetime, timezone
from supabase import create_client, Client

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("❌ Missing SUPABASE_URL or SUPABASE_KEY in environment variables")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Symbols we want to track (add more later once tested)
SYMBOLS = ["btcusdt", "ethusdt", "bnbusdt", "solusdt"]

# Binance websocket multi-stream for orderbook snapshots
STREAM_URL = "wss://fstream.binance.com/stream?streams=" + "/".join(
    [f"{s}@depth20@100ms" for s in SYMBOLS]
)

async def save_orderbook(symbol, data):
    """Save bids/asks from orderbook update into Supabase."""
    try:
        bids = data.get("bids", [])
        asks = data.get("asks", [])
        ts = datetime.fromtimestamp(data["E"] / 1000, tz=timezone.utc)

        rows = []

        # Limit to top 5 levels for debugging (expand later)
        for i, (price, qty) in enumerate(bids[:5]):
            rows.append({
                "symbol": symbol.upper(),
                "side": "BID",
                "price": float(price),
                "quantity": float(qty),
                "depth_level": i + 1,
                "time": ts
            })

        for i, (price, qty) in enumerate(asks[:5]):
            rows.append({
                "symbol": symbol.upper(),
                "side": "ASK",
                "price": float(price),
                "quantity": float(qty),
                "depth_level": i + 1,
                "time": ts
            })

        if rows:
            res = sb.table("binance_orderbook").insert(rows).execute()
            print(f"✅ Inserted {len(rows)} rows for {symbol.upper()} at {ts} → {res}")
        else:
            print(f"⚠️ No rows to insert for {symbol.upper()} at {ts}")

    except Exception as e:
        print(f"❌ Insert failed for {symbol.upper()}: {e}")


async def main():
    """Connect to Binance orderbook and stream data into Supabase."""
    async with websockets.connect(STREAM_URL) as ws:
        print("🚀 Connected to Binance Multi-Symbol Order Book WebSocket")
        while True:
            try:
                msg = await ws.recv()
                data = json.loads(msg)

                stream = data.get("stream", "")
                symbol = stream.split("@")[0]

                if "data" in data:
                    await save_orderbook(symbol, data["data"])

            except Exception as e:
                print(f"❌ Error in main loop: {e}")
                await asyncio.sleep(5)  # backoff before retry


if __name__ == "__main__":
    asyncio.run(main())

