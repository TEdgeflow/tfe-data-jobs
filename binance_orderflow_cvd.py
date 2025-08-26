import os
import time
import requests
import json
import websocket
from supabase import create_client, Client
from datetime import datetime, timezone

# ========= ENV VARS =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ========= SYMBOLS =========
SYMBOLS = ["btcusdt", "ethusdt"]  # extend later with more perps

# ========= TRACKING =========
cvd_running = {s: 0 for s in SYMBOLS}  # hold running CVD per symbol

def on_message(ws, message):
    global cvd_running
    msg = json.loads(message)
    stream = msg.get("stream")
    data = msg.get("data")

    if not data or "s" not in data:
        return

    symbol = data["s"].lower()  # e.g. BTCUSDT
    price = float(data["p"])
    qty = float(data["q"])
    is_buyer_maker = data["m"]  # True = sell, False = buy

    # classify buy vs sell
    if is_buyer_maker:  # seller initiated → counts as sell
        buys, sells = 0, qty
    else:  # buyer initiated → counts as buy
        buys, sells = qty, 0

    delta = buys - sells
    cvd_running[symbol] += delta

    row = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "symbol": symbol,
        "buys_volume": buys,
        "sells_volume": sells,
        "delta": delta,
        "cvd": cvd_running[symbol]
    }

    try:
        sb.table("orderflow_cvd").insert(row).execute()
        print(f"[upsert] {symbol} {row}")
    except Exception as e:
        print("❌ Supabase error:", e)

def on_error(ws, error):
    print("WebSocket error:", error)

def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed", close_status_code, close_msg)

def on_open(ws):
    print("✅ Connected to Binance WebSocket")
    params = [f"{s}@trade" for s in SYMBOLS]
    sub_msg = {"method": "SUBSCRIBE", "params": params, "id": 1}
    ws.send(json.dumps(sub_msg))

def main():
    url = "wss://stream.binance.com:9443/stream?streams=" + "/".join(f"{s}@trade" for s in SYMBOLS)
    ws = websocket.WebSocketApp(url, on_message=on_message, on_error=on_error, on_close=on_close, on_open=on_open)
    ws.run_forever()

if __name__ == "__main__":
    main()
