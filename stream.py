# stream.py

import argparse
import asyncio
import os
from datetime import datetime, timezone
from typing import Optional, Tuple, List

from adapters import get_adapter
from writer_csv import CSVWriter
from writer_pg import PostgresWriter  # NEW


def parse_args():
    p = argparse.ArgumentParser(description="Stream crypto liquidations → CSV and/or Postgres")
    # include 'aster' here
    p.add_argument("--exchange", choices=["binance", "bybit", "okx", "aster"])
    p.add_argument("--market", choices=["usdt", "coin"])
    p.add_argument("--outdir", help="Output directory (single-stream mode)")
    p.add_argument("--all", action="store_true")
    p.add_argument("--streams", default="")
    p.add_argument("--outdir-root", default="data")
    p.add_argument("--no-write", action="store_true")
    p.add_argument("--no-color", action="store_true")
    p.add_argument("--subscribe-chunk", type=int, default=100)
    # NEW: sinks & Postgres config
    p.add_argument("--sink", choices=["csv", "pg", "both"], default="both",
                   help="Where to write: csv, pg, or both")
    p.add_argument("--pg-dsn", default=os.environ.get("PG_DSN", ""),
                   help="Postgres DSN (e.g., postgres://user:pass@host:5432/db)")
    p.add_argument("--pg-table", default=os.environ.get("PG_TABLE", "public.liquidations"),
                   help="Postgres table name (schema.table)")
    p.add_argument("--pg-batch", type=int, default=int(os.environ.get("PG_BATCH", "500")),
                   help="Batch size for inserts")
    p.add_argument("--pg-interval", type=float, default=float(os.environ.get("PG_INTERVAL", "1.0")),
                   help="Flush interval seconds")
    return p.parse_args()


class WriterShim:
    """
    Fan-out writer: prints, then forwards to CSV and/or Postgres.
    """
    def __init__(self, outdir: str, print_colors: bool, no_write: bool,
                 csv_writer: Optional[CSVWriter], pg_writer: Optional[PostgresWriter]):
        self.no_write = no_write
        self.csv_writer = csv_writer
        self.pg_writer = pg_writer
        self.print_colors = print_colors

        # colors
        self.CLR_RED   = "\x1b[31m"
        self.CLR_GREEN = "\x1b[32m"
        self.CLR_DIM   = "\x1b[2m"
        self.CLR_RST   = "\x1b[0m"

    def write_row(self, row: dict):
        # terminal print
        side = (row.get("side") or "").lower()
        color = self.CLR_RED if side == "long" else self.CLR_GREEN if side == "short" else ""
        line = (
            f"[{row['exchange']}/{row['market']}] {row['symbol']} | "
            f"{(color + row['side'] + self.CLR_RST) if color and row.get('side') else (row.get('side') or '')} | "
            f"qty={row.get('qty')} @ {row.get('price')} "
            f"({self.CLR_DIM}notional={row.get('notional')}{self.CLR_RST})"
        )
        if not self.print_colors:
            # strip ANSI
            import re
            line = re.sub(r"\x1b\[[0-9;]*m", "", line)
        print(line)

        if self.no_write:
            return

        if self.csv_writer:
            self.csv_writer.write_row(row)

        if self.pg_writer:
            self.pg_writer.write_row(row)


def _resolve_streams(args) -> List[Tuple[str, str]]:
    pairs: List[Tuple[str, str]] = []
    if args.all:
        pairs = [
            ("binance", "usdt"), ("binance", "coin"),
            ("bybit", "usdt"),   ("bybit", "coin"),
            ("okx", "usdt"),     ("okx", "coin"),
            ("aster", "usdt"),   # NEW: aster is USDT-margined
        ]
    elif args.streams:
        for item in args.streams.split(","):
            ex, mk = item.strip().split(":")
            ex, mk = ex.lower(), mk.lower()
            # Guard: Aster is USDT-only
            if ex == "aster" and mk != "usdt":
                print("[aster] Warning: overriding market to 'usdt' (Aster is USDT-margined).")
                mk = "usdt"
            pairs.append((ex, mk))
    else:
        ex = (args.exchange or "").lower()
        mk = (args.market or "").lower()
        if ex == "aster" and mk != "usdt":
            print("[aster] Warning: overriding --market to 'usdt' (Aster is USDT-margined).")
            mk = "usdt"
        pairs = [(ex, mk)]
    return pairs


def _outdir_for(ex: str, mk: str, args) -> str:
    return args.outdir or os.path.join(args.outdir_root, f"{ex}_{mk}")


async def _run_one(ex: str, mk: str, args, pg_writer: Optional[PostgresWriter]):
    outdir = _outdir_for(ex, mk, args)
    csv_writer = None
    if args.sink in ("csv", "both") and not args.no_write:
        os.makedirs(outdir, exist_ok=True)
        csv_writer = CSVWriter(outdir)

    writer = WriterShim(
        outdir=outdir,
        print_colors=not args.no_color,
        no_write=args.no_write,
        csv_writer=csv_writer,
        pg_writer=pg_writer if (args.sink in ("pg", "both") and not args.no_write) else None
    )

    Adapter = get_adapter(ex)
    if ex == "bybit":
        adapter = Adapter(writer=writer, market=mk, symbols=None, subscribe_chunk=max(1, args.subscribe_chunk))
    else:
        # binance, okx, aster use (writer, market)
        adapter = Adapter(writer=writer, market=mk)

    print(f"[{ex}/{mk}] starting → {outdir} @ {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    await adapter.run()


async def run_all(args):
    # Init a shared PG writer if needed
    pg_writer = None
    if args.sink in ("pg", "both") and not args.no_write:
        if not args.pg_dsn:
            raise SystemExit("Postgres sink requested but --pg-dsn not provided (or PG_DSN env).")
        pg_writer = await PostgresWriter.create(
            dsn=args.pg_dsn,
            table_name=args.pg_table,
            batch_size=args.pg_batch,
            flush_interval=args.pg_interval,
        )

    try:
        pairs = _resolve_streams(args)
        tasks = [asyncio.create_task(_run_one(ex, mk, args, pg_writer)) for ex, mk in pairs]
        await asyncio.gather(*tasks)
    finally:
        if pg_writer:
            await pg_writer.aclose()


def main():
    args = parse_args()
    try:
        asyncio.run(run_all(args))
    except KeyboardInterrupt:
        print("\nShutting down…")


if __name__ == "__main__":
    main()
