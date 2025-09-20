# adapters/binance_adapter.py

import asyncio
import json
import time
from typing import Any, Dict, List, Optional

import websockets

USDTM_WSS = "wss://fstream.binance.com/ws/!forceOrder@arr"
COINM_WSS = "wss://dstream.binance.com/ws/!forceOrder@arr"

def _now_ms() -> int:
    return int(time.time() * 1000)

def _derive_liq_side(order_side: Optional[str]) -> Optional[str]:
    # Binance: BUY = forced buy to close SHORT; SELL = forced sell to close LONG
    s = (order_side or "").upper()
    if s == "BUY":
        return "short"
    if s == "SELL":
        return "long"
    return None

def _market_label(market: str) -> str:
    m = (market or "").lower()
    if m == "usdt":
        return "usdt"
    if m in ("coin", "coinm", "inverse"):
        return "coin"
    raise ValueError(f"Unknown Binance market: {market}")

class BinanceAdapter:
    EXCHANGE = "binance"

    def __init__(self, writer, market: str):
        self.writer = writer
        self.market = _market_label(market)
        self.ws_url = USDTM_WSS if self.market == "usdt" else COINM_WSS

    def _normalize_and_write_batch(self, payload: Any):
        """
        Binance !forceOrder@arr pushes an array of events. Each event looks like:
        {
          "e":"forceOrder","E":1710000000000,
          "o":{
            "s":"BTCUSDT","S":"SELL","o":"LIMIT","f":"IOC",
            "q":"0.010","p":"62000.00","ap":"61990.10","X":"FILLED",
            "l":"0.010","z":"0.010","T":1710000000123
          }
        }
        We prefer:
          - event time: ev["E"] (ms) or o["T"]
          - price: o["ap"] (avg fill) fallback o["p"]
          - qty: o["l"] (last fill) fallback o["z"] (cum) fallback o["q"]
          - symbol: o["s"]
          - order side: o["S"] => map to liq_side (long/short liquidated)
        """
        ts_ingest = _now_ms()
        events: List[Dict[str, Any]] = payload if isinstance(payload, list) else [payload]
        for ev in events:
            try:
                o = ev.get("o") or {}
                if not o:
                    continue

                event_ms = None
                if ev.get("E") is not None:
                    event_ms = int(ev["E"])
                elif o.get("T") is not None:
                    event_ms = int(o["T"])

                price = float(o.get("ap") or o.get("p") or 0.0)
                qty   = float(o.get("l") or o.get("z") or o.get("q") or 0.0)
                symbol = o.get("s", "")
                order_side = o.get("S")  # BUY/SELL
                liq_side = _derive_liq_side(order_side)
                notional = price * qty if price and qty else None

                out = {
                    "exchange": self.EXCHANGE,
                    "market": self.market,      # "usdt" or "coin"
                    "symbol": symbol,
                    "side": liq_side,           # "long" or "short" (positions liquidated)
                    "qty": qty,
                    "price": price,
                    "notional": notional,
                    "ts_exch_ms": event_ms,
                    "ts_ingest_ms": ts_ingest,
                    "raw": json.dumps(ev, separators=(",", ":"))
                }
                self.writer.write_row(out)
            except Exception as e:
                print(f"[binance] Error normalizing event: {e}")

    async def run(self):
        print(f"[binance] Connecting {self.ws_url} (market={self.market})")
        backoff = 1.0
        while True:
            try:
                async with websockets.connect(
                    self.ws_url,
                    ping_interval=20,
                    ping_timeout=10,
                    max_size=10_000_000
                ) as ws:
                    print("[binance] Connected.")
                    backoff = 1.0  # reset on success

                    async for msg in ws:
                        try:
                            if isinstance(msg, bytes):
                                msg = msg.decode("utf-8", "ignore")
                            if msg == "ping":
                                await ws.send("pong")
                                continue
                            data = json.loads(msg)
                            self._normalize_and_write_batch(data)
                        except json.JSONDecodeError:
                            # ignore keepalives or unexpected frames
                            continue
                        except Exception as e:
                            print(f"[binance] Frame error: {e}")
                            continue
            except Exception as e:
                print(f"[binance] WS error: {e}. Reconnecting in {backoff:.1f}s...")
                await asyncio.sleep(backoff)
                backoff = min(30.0, backoff * 1.8)
                continue
