# adapters/binance.py
import json
import time
from datetime import datetime, timezone
from typing import Dict, Iterator, Optional

import websocket  # websocket-client

USDTM_WSS = "wss://fstream.binance.com/ws/!forceOrder@arr"
COINM_WSS = "wss://dstream.binance.com/ws/!forceOrder@arr"


def _ts_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def _derive_liq_side(side: str) -> Optional[str]:
    # BUY = forced buy to close a SHORT; SELL = forced sell to close a LONG
    if side == "BUY":
        return "short"
    if side == "SELL":
        return "long"
    return None


def stream(market: str = "usdt") -> Iterator[Dict]:
    """
    Yield unified liquidation events:

    {
      "ts_iso","ts_ms","exchange","market","symbol",
      "side","liq_side","price","qty","notional","raw"
    }
    """
    wss = USDTM_WSS if market == "usdt" else COINM_WSS
    label = "usdtm" if market == "usdt" else "coinm"

    backoff = 1.0
    max_backoff = 30.0

    while True:
        ws = None
        try:
            ws = websocket.create_connection(wss, timeout=20)
            while True:
                msg = ws.recv()
                if not msg:
                    raise websocket.WebSocketConnectionClosedException("empty message")

                try:
                    payload = json.loads(msg)
                except json.JSONDecodeError:
                    continue

                events = payload if isinstance(payload, list) else [payload]
                for ev in events:
                    o = ev.get("o", {})
                    if not o:
                        continue

                    event_ms = int(ev.get("E") or o.get("T") or 0)
                    price = float(o.get("ap") or o.get("p") or 0.0)
                    qty = float(o.get("l") or o.get("z") or o.get("q") or 0.0)

                    yield {
                        "ts_iso": _ts_iso(event_ms) if event_ms else datetime.now(timezone.utc).isoformat(),
                        "ts_ms": event_ms,
                        "exchange": "binance",
                        "market": label,
                        "symbol": o.get("s", ""),
                        "side": o.get("S", ""),
                        "liq_side": _derive_liq_side(o.get("S", "")),
                        "price": price,
                        "qty": qty,
                        "notional": price * qty,
                        "raw": ev,
                    }
        except Exception:
            try:
                if ws:
                    ws.close()
            except Exception:
                pass
            time.sleep(backoff)
            backoff = min(max_backoff, backoff * 1.8)
            continue
