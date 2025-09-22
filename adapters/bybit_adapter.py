# adapters/bybit_adapter.py
import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import List, Optional

import websockets
import requests


def _now_ms() -> int:
    return int(time.time() * 1000)


def _to_float(x) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0


class BybitAdapter:
    """
    Streams Bybit liquidations via v5 public WS.

    Defaults to the newer per-symbol channel:
      - allLiquidation.<SYMBOL>

    You can opt into the legacy channel by passing use_all=False:
      - liquidation.<SYMBOL>

    Market mapping:
      - market == "usdt"   -> linear  (wss://stream.bybit.com/v5/public/linear)
      - market in {"coin","coinm","inverse"} -> inverse (wss://stream.bybit.com/v5/public/inverse)
    """

    EXCHANGE = "bybit"

    def __init__(
        self,
        writer,
        market: str = "usdt",
        symbols: Optional[List[str]] = None,
        subscribe_chunk: int = 100,
        use_all: bool = True,
    ):
        self.writer = writer
        self.market = (market or "").lower()  # "usdt" or "coin"
        self.symbols = symbols or []
        self.subscribe_chunk = max(1, int(subscribe_chunk))
        self.use_all = bool(use_all)

        if self.market == "usdt":
            self.ws_url = "wss://stream.bybit.com/v5/public/linear"
            self.category = "linear"
        elif self.market in {"coin", "coinm", "inverse"}:
            self.ws_url = "wss://stream.bybit.com/v5/public/inverse"
            self.category = "inverse"
        else:
            raise ValueError(f"Unknown Bybit market: {market}")

    # -------------------- Public entrypoint --------------------

    async def run(self):
        if not self.symbols:
            self.symbols = await self._fetch_symbols()
        if not self.symbols:
            logging.error(f"[bybit/{self.market}] No symbols discovered; exiting.")
            return

        backoff = 1.0
        while True:
            try:
                logging.info(f"[bybit/{self.market}] Connecting: {self.ws_url}")
                async with websockets.connect(
                    self.ws_url, ping_interval=20, ping_timeout=10, max_size=10_000_000
                ) as ws:
                    logging.info(
                        f"[bybit/{self.market}] Connected. Subscribing to {len(self.symbols)} symbols "
                        f"via {'allLiquidation' if self.use_all else 'liquidation'} (chunk={self.subscribe_chunk})."
                    )
                    await self._subscribe(ws, self.symbols)

                    # Reset backoff after a successful connect
                    backoff = 1.0

                    async for msg in ws:
                        if isinstance(msg, bytes):
                            msg = msg.decode("utf-8", "ignore")

                        # Bybit sends JSON frames
                        try:
                            data = json.loads(msg)
                        except json.JSONDecodeError:
                            continue

                        await self._handle_message(data)

            except Exception as e:
                logging.error(f"[bybit/{self.market}] WS error: {e}. Reconnecting in {backoff:.1f}s...")
                await asyncio.sleep(backoff)
                backoff = min(30.0, backoff * 1.8)

    # -------------------- Internals --------------------

    async def _fetch_symbols(self) -> List[str]:
        """Fetch all symbols for the selected category (linear/inverse)."""
        url = "https://api.bybit.com/v5/market/instruments-info"
        params = {"category": self.category}
        try:
            r = requests.get(url, params=params, timeout=20)
            r.raise_for_status()
            data = r.json()
            items = (data or {}).get("result", {}).get("list", []) or []
            symbols = [it["symbol"] for it in items if it.get("symbol")]
            logging.info(f"[bybit/{self.market}] Discovered {len(symbols)} symbols for {self.category}.")
            return symbols
        except Exception as e:
            logging.error(f"[bybit/{self.market}] Error fetching symbols: {e}")
            return []

    async def _subscribe(self, ws, symbols: List[str]):
        """Subscribe in chunks to avoid payload/limit issues."""
        prefix = "allLiquidation" if self.use_all else "liquidation"
        total = len(symbols)
        sent = 0

        for i in range(0, total, self.subscribe_chunk):
            chunk = symbols[i : i + self.subscribe_chunk]
            args = [f"{prefix}.{s}" for s in chunk]
            sub = {"op": "subscribe", "args": args}
            await ws.send(json.dumps(sub))
            sent += len(chunk)
            logging.info(f"[bybit/{self.market}] Subscribed {sent}/{total}")
            # Try to read an ack to keep the buffer clean (don't block forever)
            try:
                ack = await asyncio.wait_for(ws.recv(), timeout=3)
                # Optional: logging.debug(f"[bybit/{self.market}] Sub ack: {ack}")
            except asyncio.TimeoutError:
                pass
            await asyncio.sleep(0.1)  # gentle pacing

    async def _handle_message(self, data: dict):
        """Dispatch per topic and normalize."""
        topic = data.get("topic", "")
        if not topic:
            return

        # New channel: allLiquidation.<SYMBOL>  (data: list of compact rows)
        if topic.startswith("allLiquidation."):
            rows = data.get("data") or []
            msg_ts = data.get("ts")
            for liq in rows:
                self._process_liquidation_any(liq, msg_ts)
            return

        # Legacy channel: liquidation.<SYMBOL>  (data: dict or list)
        if topic.startswith("liquidation."):
            rows = data.get("data")
            if rows is None:
                return
            msg_ts = data.get("ts")
            if isinstance(rows, dict):
                self._process_liquidation_any(rows, msg_ts)
            else:
                for liq in rows:
                    self._process_liquidation_any(liq, msg_ts)
            return

    def _process_liquidation_any(self, liq: dict, msg_ts: Optional[int]):
        """
        Normalize both Bybit schemas:

        New allLiquidation rows (compact keys):
          { "T": 1739502302929, "s": "ROSEUSDT", "S": "Sell", "v": "20000", "p": "0.04499" }

        Legacy liquidation rows:
          { "updatedTimeE6": "...", "symbol": "BTCUSDT", "side": "Buy"/"Sell", "size": "0.01", "price": "30000" }

        Emits unified schema required by WriterShim/CSVWriter.
        """
        try:
            # Symbol
            symbol = liq.get("s") or liq.get("symbol") or ""

            # Side mapping -> which positions were liquidated:
            #   "Buy"  => shorts liquidated (forced buy to cover)
            #   "Sell" => longs  liquidated (forced sell to close)
            side_raw = (liq.get("S") or liq.get("side") or "").lower()
            liq_side = "short" if side_raw == "buy" else "long" if side_raw == "sell" else ""

            # Qty / Price
            qty = _to_float(liq.get("v") or liq.get("size") or 0)
            price = _to_float(liq.get("p") or liq.get("price") or 0)
            notional = price * qty if price and qty else 0.0

            # Timestamps (ms)
            ts_exch_ms = None
            if liq.get("T") is not None:  # new schema ms
                ts_exch_ms = int(liq["T"])
            elif liq.get("updatedTimeE6") is not None:  # legacy µs
                ts_exch_ms = int(int(liq["updatedTimeE6"]) / 1000)
            elif msg_ts is not None:
                ts_exch_ms = int(msg_ts)

            out = {
                "exchange": self.EXCHANGE,
                "market": self.market,         # "usdt" or "coin"
                "symbol": symbol,
                "side": liq_side,              # "long" | "short"
                "qty": qty,
                "price": price,
                "notional": notional,
                "ts_exch_ms": ts_exch_ms,
                "ts_ingest_ms": _now_ms(),
                "raw": json.dumps(liq, separators=(",", ":")),
            }

            # Terminal print (coloring handled by WriterShim if you added it)
            # print(f"[bybit/{self.market}] {symbol} | {liq_side} | qty={qty} @ {price} (notional={notional})")

            self.writer.write_row(out)

        except Exception as e:
            logging.error(f"[bybit/{self.market}] normalize error: {e} :: {liq}")
