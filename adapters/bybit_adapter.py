# adapters/bybit_adapter.py

import asyncio
import json
import math
import time
from typing import Iterable, List, Dict, Any, Optional

import websockets
import requests

BYBIT_REST = "https://api.bybit.com/v5/market/instruments-info"
BYBIT_WS_LINEAR  = "wss://stream.bybit.com/v5/public/linear"
BYBIT_WS_INVERSE = "wss://stream.bybit.com/v5/public/inverse"

# ---- Small helpers ----

def _now_ms() -> int:
    return int(time.time() * 1000)

def _chunked(it: Iterable[Any], n: int) -> Iterable[List[Any]]:
    buf = []
    for x in it:
        buf.append(x)
        if len(buf) >= n:
            yield buf
            buf = []
    if buf:
        yield buf

def _category_for_market(market: str) -> str:
    # USDT-M = "linear", COIN-M = "inverse"
    m = (market or "").lower()
    if m == "usdt":
        return "linear"
    if m in ("coin", "coinm", "inverse"):
        return "inverse"
    raise ValueError(f"Unknown Bybit market: {market}")

def _ws_url_for_category(category: str) -> str:
    return BYBIT_WS_LINEAR if category == "linear" else BYBIT_WS_INVERSE

# ---- Adapter ----

class BybitAdapter:
    EXCHANGE = "bybit"

    def __init__(self, writer, market: str, symbols: Optional[List[str]] = None, subscribe_chunk: int = 100):
        self.writer = writer
        self.market = market.lower()
        self.category = _category_for_market(self.market)
        self.ws_url = _ws_url_for_category(self.category)
        self.subscribe_chunk = max(1, subscribe_chunk)
        self.symbols = symbols or []

    def _fetch_symbols(self) -> List[str]:
        if self.symbols:
            return self.symbols
        params = {"category": self.category}
        try:
            r = requests.get(BYBIT_REST, params=params, timeout=20)
            r.raise_for_status()
            data = r.json()
            items = (data or {}).get("result", {}).get("list", []) or []
            # Symbols look like "BTCUSDT", "ETHUSD", etc.
            return [it["symbol"] for it in items if it.get("symbol")]
        except Exception as e:
            print(f"[bybit] Error fetching symbols: {e}")
            return []

    async def _subscribe(self, ws, symbols: List[str]):
        # Topic name: liquidation.{symbol}
        for batch in _chunked(symbols, self.subscribe_chunk):
            args = [f"liquidation.{sym}" for sym in batch]
            sub = {"op": "subscribe", "args": args}
            await ws.send(json.dumps(sub))
            # read ack(s) – Bybit sends a single ack; we won’t block forever
            try:
                ack = await asyncio.wait_for(ws.recv(), timeout=5)
                # optional: print(f"[bybit] Sub ack: {ack}")
            except asyncio.TimeoutError:
                pass
            await asyncio.sleep(0.1)  # gentle pacing

    def _normalize_and_write(self, msg: Dict[str, Any]):
        """
        Bybit v5 liquidation payload example (public):
        {
          "topic":"liquidation.BTCUSDT",
          "type":"snapshot",
          "ts": 1700000000000,
          "data":[
             {"updatedTimeE6":..., "price":"...", "size":"...", "side":"Buy"/"Sell", "symbol":"BTCUSDT"}
          ]
        }
        Some gateways deliver single dict under "data". Handle both.
        """
        try:
            topic = msg.get("topic", "")
            if not topic.startswith("liquidation."):
                return

            d = msg.get("data")
            if not d:
                return

            rows = d if isinstance(d, list) else [d]
            ts_msg = msg.get("ts")  # message server timestamp (ms)
            ts_ingest = _now_ms()

            for row in rows:
                symbol = row.get("symbol")
                # Bybit "side" is the aggressor direction of the liquidation (taker?). We map to pos side:
                # Convention (common in feeds):
                # - If side == "Buy" => SHORTs liquidated (buy to cover)
                # - If side == "Sell" => LONGs liquidated (sell to close)
                side_raw = (row.get("side") or "").lower()
                liq_side = "short" if side_raw == "buy" else "long" if side_raw == "sell" else side_raw

                # sizes come as strings; cast as float
                price = float(row.get("price") or 0.0)
                qty   = float(row.get("size") or 0.0)
                notional = price * qty if price and qty else None

                ts_exch = None
                for key in ("updatedTimeE6", "updatedTime", "ts", "time"):
                    v = row.get(key)
                    if v is not None:
                        # updatedTimeE6 is µs
                        if key == "updatedTimeE6":
                            ts_exch = int(int(v) / 1000)
                        else:
                            ts_exch = int(v)
                        break
                if ts_exch is None and ts_msg:
                    ts_exch = int(ts_msg)

                out = {
                    "exchange": self.EXCHANGE,
                    "market": self.market,          # "usdt" or "coin"
                    "symbol": symbol,
                    "side": liq_side,               # "long" or "short"
                    "qty": qty,
                    "price": price,
                    "notional": notional,
                    "ts_exch_ms": ts_exch,
                    "ts_ingest_ms": ts_ingest,
                    "raw": json.dumps(row, separators=(",", ":"))
                }
                self.writer.write_row(out)
        except Exception as e:
            print(f"[bybit] Error normalizing message: {e}")

    async def run(self):
        symbols = self._fetch_symbols()
        if not symbols:
            print("[bybit] No symbols found; exiting.")
            return

        print(f"[bybit] Connecting {self.ws_url} for {len(symbols)} symbols ({self.category}).")
        async for ws in websockets.connect(self.ws_url, ping_interval=20, ping_timeout=10, max_size=10_000_000):
            try:
                await self._subscribe(ws, symbols)
                while True:
                    msg = await ws.recv()
                    if isinstance(msg, bytes):
                        msg = msg.decode("utf-8", "ignore")
                    if msg == "ping":
                        await ws.send("pong")
                        continue
                    data = json.loads(msg)
                    self._normalize_and_write(data)
            except Exception as e:
                print(f"[bybit] WS error: {e}; reconnecting in 3s...")
                await asyncio.sleep(3)
                continue
