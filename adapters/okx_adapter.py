# adapters/okx_adapter.py

import asyncio
import json
import time
from typing import Any, Dict, List

import websockets

OKX_WS = "wss://ws.okx.com:8443/ws/v5/public"

def _now_ms() -> int:
    return int(time.time() * 1000)

def _okx_is_usdtm(inst_id: str) -> bool:
    # e.g., "BTC-USDT-SWAP"
    return inst_id.endswith("-USDT-SWAP") or inst_id.endswith("-USDC-SWAP")

def _okx_is_coinm(inst_id: str) -> bool:
    # e.g., "BTC-USD-SWAP"
    return inst_id.endswith("-USD-SWAP")

class OKXAdapter:
    EXCHANGE = "okx"

    def __init__(self, writer, market: str):
        self.writer = writer
        self.market = (market or "").lower()  # "usdt" or "coin"

    async def _subscribe(self, ws):
        sub = {
            "op": "subscribe",
            "args": [{"channel": "liquidation-orders", "instType": "SWAP"}]
        }
        await ws.send(json.dumps(sub))
        # read ack
        try:
            ack = await asyncio.wait_for(ws.recv(), timeout=5)
            # optional: print(f"[okx] Sub ack: {ack}")
        except asyncio.TimeoutError:
            pass

    def _normalize_and_write(self, msg: Dict[str, Any]):
        """
        OKX 'liquidation-orders' payload looks like:
        {
          "arg":{"channel":"liquidation-orders","instType":"SWAP"},
          "data":[
            {
              "instType":"SWAP","instId":"BTC-USDT-SWAP",
              "details":[
                {"posSide":"long","side":"sell","bkPx":"...","fillPx":"...","sz":"...","ts":"..."},
                ...
              ]
            }, ...
          ]
        }
        """
        try:
            arg = msg.get("arg", {}) or {}
            if arg.get("channel") != "liquidation-orders":
                return
            data = msg.get("data") or []
            if not data:
                return

            ts_ingest = _now_ms()
            for liq in data:
                inst_id = liq.get("instId", "")
                if self.market == "usdt" and not _okx_is_usdtm(inst_id):
                    continue
                if self.market in ("coin", "coinm", "inverse") and not _okx_is_coinm(inst_id):
                    continue

                # OKX groups multiple liquidations by instrument under "details"
                details = liq.get("details") or []
                for d in details:
                    # Map OKX sides to "long"/"short" liquidations
                    # posSide: "long"/"short"
                    # side:    "buy"/"sell" (direction of the liquidation trade)
                    # We report which positions are being liquidated using posSide.
                    pos_side = (d.get("posSide") or "").lower()
                    liq_side = pos_side if pos_side in ("long", "short") else ""

                    price = float(d.get("fillPx") or d.get("bkPx") or 0.0)
                    qty   = float(d.get("sz") or 0.0)
                    notional = price * qty if price and qty else None

                    ts_exch = None
                    if d.get("ts"):
                        ts_exch = int(d["ts"])

                    out = {
                        "exchange": self.EXCHANGE,
                        "market": self.market,
                        "symbol": inst_id,         # you can map to "BTCUSDT" later if you prefer
                        "side": liq_side,          # long/short liquidated
                        "qty": qty,
                        "price": price,
                        "notional": notional,
                        "ts_exch_ms": ts_exch,
                        "ts_ingest_ms": ts_ingest,
                        "raw": json.dumps(d, separators=(",", ":"))
                    }
                    self.writer.write_row(out)
        except Exception as e:
            print(f"[okx] Error normalizing: {e}")

    async def run(self):
        print(f"[okx] Connecting {OKX_WS} (market={self.market})")
        async for ws in websockets.connect(OKX_WS, ping_interval=20, ping_timeout=10, max_size=10_000_000):
            try:
                await self._subscribe(ws)
                while True:
                    msg = await ws.recv()
                    if isinstance(msg, bytes):
                        msg = msg.decode("utf-8", "ignore")
                    # OKX ping/pong: sometimes "ping" string or JSON "event":"ping"
                    if msg == "ping":
                        await ws.send("pong")
                        continue
                    data = json.loads(msg)
                    if data.get("event") == "pong":
                        continue
                    self._normalize_and_write(data)
            except Exception as e:
                print(f"[okx] WS error: {e}; reconnecting in 3s...")
                await asyncio.sleep(3)
                continue
