#!/usr/bin/env python3
"""
Binance Futures Liquidations → CSV

- Streams liquidation ("forceOrder") events from Binance Futures WebSocket.
- Supports USDT-M (fstream) and COIN-M (dstream).
- Optional symbol filter.
- Robust CSV with headers and computed notional.
- Auto-reconnect with backoff; Ctrl+C to exit cleanly.

Notes on directions:
- On Binance liquidation events, "BUY" generally corresponds to SHORT liquidations (forced buy to close short),
  and "SELL" corresponds to LONG liquidations. We record a derived field 'liq_side' = 'short' or 'long' using that rule.
"""

import argparse
import csv
import json
import sys
import time
from datetime import datetime, timezone
from typing import Optional

import websocket  # websocket-client

USDTM_WSS = "wss://fstream.binance.com/ws/!forceOrder@arr"
COINM_WSS = "wss://dstream.binance.com/ws/!forceOrder@arr"

CSV_HEADERS = [
    "ts_iso",        # ISO timestamp (server event time)
    "ts_ms",         # event time (ms)
    "exchange",      # 'binance'
    "market",        # 'usdtm' or 'coinm'
    "symbol",        # e.g., BTCUSDT or BTCUSD_PERP
    "side",          # raw side from event: BUY/SELL
    "liq_side",      # derived: 'short' if side=BUY else 'long' if side=SELL
    "price",         # executed price (float)
    "qty",           # executed qty in base asset (float)
    "notional",      # price * qty (float)
    "order_status",  # X (e.g., FILLED)
]

def parse_args():
    p = argparse.ArgumentParser(
        description="Stream Binance Futures liquidation events to CSV."
    )
    p.add_argument("--market", choices=["usdt", "coin"], default="usdt",
                   help="Which futures market to stream: USDT-M ('usdt') or COIN-M ('coin'). Default: usdt")
    p.add_argument("--symbol", type=str, default=None,
                   help="Optional symbol filter (e.g., BTCUSDT or BTCUSD_PERP). Case-sensitive, matches Binance symbol exactly.")
    p.add_argument("--outfile", type=str, default="liquidations.csv",
                   help="CSV output file path. Default: liquidations.csv")
    p.add_argument("--append", action="store_true",
                   help="Append to existing file (don’t rewrite header if file exists).")
    p.add_argument("--quiet", action="store_true",
                   help="Less console logging.")
    return p.parse_args()

def ts_ms_to_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()

def derive_liq_side(side: str) -> Optional[str]:
    # BUY → short liquidation, SELL → long liquidation
    if side == "BUY":
        return "short"
    if side == "SELL":
        return "long"
    return None

def extract_row(event: dict, market_label: str):
    """
    Binance 'forceOrder' event structure (array wrapper from !forceOrder@arr):
    {
      "e":"forceOrder",
      "E":159...,           # event time (ms)
      "o":{
         "s":"BTCUSDT",     # symbol
         "S":"SELL",        # side
         "o":"LIMIT",
         "f":"IOC",
         "q":"0.009",       # order qty
         "p":"9425.21",     # order price
         "ap":"9425.21",    # avg price (executed)
         "X":"FILLED",      # order status
         "l":"0.009",       # last filled qty
         "z":"0.009",       # cumulative filled qty
         "T":159...         # timestamp (ms)
      }
    }
    We'll prefer executed/avg price 'ap' and last filled qty 'l'; fallback to 'p'/'q' if missing.
    """
    e = event
    order = e.get("o", {})

    event_ms = int(e.get("E", 0)) if e.get("E") is not None else int(order.get("T", 0) or 0)
    symbol = order.get("s", "")
    side = order.get("S", "")
    price = order.get("ap") or order.get("p") or "0"
    qty = order.get("l") or order.get("z") or order.get("q") or "0"
    status = order.get("X", "")

    try:
        price_f = float(price)
    except Exception:
        price_f = 0.0
    try:
        qty_f = float(qty)
    except Exception:
        qty_f = 0.0

    notional = price_f * qty_f

    return {
        "ts_iso": ts_ms_to_iso(event_ms) if event_ms else datetime.now(timezone.utc).isoformat(),
        "ts_ms": event_ms,
        "exchange": "binance",
        "market": market_label,
        "symbol": symbol,
        "side": side,
        "liq_side": derive_liq_side(side),
        "price": price_f,
        "qty": qty_f,
        "notional": notional,
        "order_status": status,
    }

def ensure_header(writer: csv.DictWriter, file_obj, wrote_header_flag: list):
    if not wrote_header_flag[0]:
        writer.writeheader()
        file_obj.flush()
        wrote_header_flag[0] = True

def main():
    args = parse_args()
    wss = USDTM_WSS if args.market == "usdt" else COINM_WSS
    market_label = "usdtm" if args.market == "usdt" else "coinm"

    # Open CSV (append or write)
    mode = "a" if args.append else "w"
    wrote_header = [False]

    # If appending and file already exists with header, we leave wrote_header False; DictWriter will check once.
    # Simpler approach: always (re)write header in 'w' mode; write once in 'a' if file empty.
    # We'll attempt a lightweight check in append mode:
    if args.append:
        try:
            with open(args.outfile, "r", newline="", encoding="utf-8") as f:
                has_any = f.read(1)
                if has_any:
                    wrote_header[0] = True
        except FileNotFoundError:
            pass

    backoff = 1.0
    max_backoff = 30.0

    print(f"[i] Connecting to {wss}  (market={market_label})")
    print(f"[i] Writing CSV → {args.outfile}  (append={args.append})")
    if args.symbol:
        print(f"[i] Symbol filter: {args.symbol}")

    try:
        with open(args.outfile, mode, newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)

            if not args.append:
                ensure_header(writer, f, wrote_header)

            while True:
                ws = None
                try:
                    ws = websocket.create_connection(wss, timeout=20)
                    if not args.quiet:
                        print("[i] Connected. Streaming liquidation events… (Ctrl+C to stop)")
                    backoff = 1.0  # reset backoff on success

                    while True:
                        msg = ws.recv()
                        if not msg:
                            raise websocket.WebSocketConnectionClosedException("Empty message / connection closed")

                        # The arr stream returns a JSON array of forceOrder objects
                        try:
                            data = json.loads(msg)
                        except json.JSONDecodeError:
                            if not args.quiet:
                                print("[warn] JSON decode error; skipping message")
                            continue

                        # Normalize to list
                        events = data if isinstance(data, list) else [data]
                        for ev in events:
                            # Some payloads may wrap forceOrder under key; handle both
                            if isinstance(ev, dict) and ev.get("e") == "forceOrder":
                                row = extract_row(ev, market_label)
                            elif isinstance(ev, dict) and "o" in ev:
                                row = extract_row(ev, market_label)
                            else:
                                continue

                            if args.symbol and row["symbol"] != args.symbol:
                                continue

                            ensure_header(writer, f, wrote_header)
                            writer.writerow(row)
                            if not args.quiet:
                                print(f"{row['ts_iso']} {row['symbol']} {row['liq_side']} "
                                      f"{row['qty']} @ {row['price']} → {row['notional']:.2f}")
                except KeyboardInterrupt:
                    print("\n[✓] Stopped by user.")
                    return
                except Exception as e:
                    if not args.quiet:
                        print(f"[warn] Connection error: {e}. Reconnecting in {backoff:.1f}s…")
                    try:
                        if ws:
                            ws.close()
                    except Exception:
                        pass
                    time.sleep(backoff)
                    backoff = min(max_backoff, backoff * 1.8)
    except KeyboardInterrupt:
        print("\n[✓] Stopped by user.")
    except Exception as e:
        print(f"[err] Failed to open CSV '{args.outfile}': {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
