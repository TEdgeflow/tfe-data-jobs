# ingesters/binance_liquidations_ingest.py

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

BINANCE_WS_URL = "wss://fstream.binance.com/ws/!forceOrder@arr"

async def save_liquidation(liq):
    try:
        o = liq["o"]
        row = {
            "symbol": o["s"],
            "side": o["S"],  # BUY or SELL
            "price": float(o["ap"]),
            "quantity": float(o["q"]),
            "time": datetime.fromtimestamp(o["T"] / 1000, tz=timezone.utc).isoformat(),
