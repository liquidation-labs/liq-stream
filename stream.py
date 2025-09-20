# stream.py
import argparse
import asyncio
import os
import sys
from datetime import datetime, timezone
from typing import List, Tuple

from adapters import get_adapter
from writer_csv import CSVWriter

# ---------- Pretty terminal output (optional colors) ----------
CLR_RED   = "\x1b[31m"
CLR_GREEN = "\x1b[32m"
CLR_DIM   = "\x1b[2m"
CLR_RST   = "\x1b[0m"

# ---------- Writer shim prints + writes ----------
class WriterShim:
    def __init__(self, outdir: str, print_colors: bool = True, no_write: bool = False):
        self.no_write = no_write
        self.print_colors = print_colors
        if not no_write:
            os.makedirs(outdir, exist_ok=True)
            self.csv = CSVWriter(outdir)

    def write_row(self, row: dict):
        # terminal line
        side = (row.get("side") or "").lower()
        color = ""
        if self.print_colors:
            color = CLR_RED if side == "long" else CLR_GREEN if side == "short" else ""
        line = (
            f"[{row['exchange']}/{row['market']}] {row['symbol']} | "
            f"{color}{row['side']}{CLR_RST if color else ''} | "
            f"qty={row['qty']} @ {row['price']} "
            f"({CLR_DIM}notional={row['notional']}{CLR_RST})"
        )
        print(line)
        # csv
        if not self.no_write:
            self.csv.write_row(row)

# ---------- CLI ----------
def parse_args():
    p = argparse.ArgumentParser(description="Stream crypto liquidations to CSV (+ log to terminal)")
    # single-stream mode (backwards compatible)
    p.add_argument("--exchange", choices=["binance", "bybit", "okx"], help="Exchange to stream")
    p.add_argument("--market", choices=["usdt", "coin"], help="Market type (USDT-margined or COIN-margined)")
    p.add_argument("--outdir", help="Output directory (single-stream mode)")
    # multi-stream mode
    p.add_argument("--all", action="store_true", help="Run ALL exchanges/markets at once (6 streams)")
    p.add_argument("--streams", default="", help="Comma list: e.g. binance:usdt,bybit:coin,okx:usdt")
    p.add_argument("--outdir-root", default="data", help="Root output dir when running multiple streams")
    # behavior
    p.add_argument("--no-write", action="store_true", help="Print only; do not write CSV files")
    p.add_argument("--no-color", action="store_true", help="Disable ANSI colors in terminal prints")
    # Bybit tuning
    p.add_argument("--subscribe-chunk", type=int, default=100, help="Bybit: symbols per subscribe request")
    return p.parse_args()

# ---------- Helpers ----------
def _resolve_streams(args) -> List[Tuple[str, str]]:
    """
    Returns a list of (exchange, market) pairs.
    Priority:
      1) --all
      2) --streams
      3) legacy single: --exchange + --market
    """
    pairs: List[Tuple[str, str]] = []
    if args.all:
        pairs = [
            ("binance", "usdt"), ("binance", "coin"),
            ("bybit", "usdt"),   ("bybit", "coin"),
            ("okx", "usdt"),     ("okx", "coin"),
        ]
    elif args.streams:
        for item in args.streams.split(","):
            item = item.strip()
            if not item:
                continue
            try:
                ex, mk = item.split(":")
                ex, mk = ex.strip().lower(), mk.strip().lower()
                if ex not in {"binance", "bybit", "okx"} or mk not in {"usdt", "coin"}:
                    raise ValueError
                pairs.append((ex, mk))
            except ValueError:
                print(f"Invalid --streams entry: '{item}'. Use exchange:market (e.g., bybit:usdt).", file=sys.stderr)
                sys.exit(2)
    else:
        # single mode
        if not (args.exchange and args.market):
            print("Either use --all, or --streams, or provide --exchange and --market.", file=sys.stderr)
            sys.exit(2)
        pairs = [(args.exchange, args.market)]
    return pairs

def _outdir_for(ex: str, mk: str, args) -> str:
    if args.outdir:
        # single-stream mode honors --outdir if provided
        return args.outdir
    # multi: partition under root
    return os.path.join(args.outdir_root, f"{ex}_{mk}")

async def _run_one(ex: str, mk: str, args):
    outdir = _outdir_for(ex, mk, args)
    writer = WriterShim(outdir, print_colors=not args.no_color, no_write=args.no_write)

    AdapterCls = get_adapter(ex)
    # pass Bybit subscribe_chunk only where relevant
    if ex == "bybit":
        adapter = AdapterCls(writer=writer, market=mk, symbols=None, subscribe_chunk=max(1, args.subscribe_chunk))
    else:
        adapter = AdapterCls(writer=writer, market=mk)

    header = f"[{ex}/{mk}]"
    print(f"{header} starting → writing to {outdir}  ({datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')})")
    await adapter.run()

async def run_all(args):
    pairs = _resolve_streams(args)
    print("Launching streams:", ", ".join([f"{ex}:{mk}" for ex, mk in pairs]))
    tasks = [asyncio.create_task(_run_one(ex, mk, args)) for ex, mk in pairs]
    # if any task crashes, we keep others running; gather with return_exceptions
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
    # surface the first exception (if any)
    for d in done:
        exc = d.exception()
        if exc:
            print(f"Task crashed: {exc}", file=sys.stderr)
    # keep process alive while others run
    await asyncio.gather(*tasks, return_exceptions=True)

def main():
    args = parse_args()
    mode = "ALL" if args.all else ("MULTI" if args.streams else "SINGLE")
    print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}] Mode: {mode}")
    try:
        asyncio.run(run_all(args))
    except KeyboardInterrupt:
        print("\nShutting down…")

if __name__ == "__main__":
    main()
