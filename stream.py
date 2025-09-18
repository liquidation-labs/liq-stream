# stream.py
import argparse
from adapters import binance as binance_adapter
from writer_csv import CsvDailyRotator


def fmt_line(ev):
    ts = ev["ts_iso"][11:19]  # HH:MM:SS
    side = (ev["liq_side"] or "").upper()
    return (f"[LIQUIDATION] {ts} | {ev['symbol']} | {side} | "
            f"Qty: {ev['qty']} | Price: {ev['price']:.4f} | Notional: ${ev['notional']:.2f}")


def main():
    p = argparse.ArgumentParser(description="Liquidation Labs multi-exchange streamer (Binance now, more soon)")
    p.add_argument("--exchange", choices=["binance"], default="binance", help="Exchange adapter to use.")
    p.add_argument("--market", choices=["usdt", "coin"], default="usdt",
                   help="Binance: usdt (USDT-M), coin (COIN-M).")
    p.add_argument("--outdir", default="data", help="Directory for CSV outputs (rotated by date).")
    p.add_argument("--prefix", default="liquidations", help="Filename prefix (prefix_YYYY-MM-DD.csv).")
    p.add_argument("--quiet", action="store_true", help="Reduce console logging.")
    args = p.parse_args()

    # Select adapter (only Binance for now)
    if args.exchange == "binance":
        stream_iter = binance_adapter.stream(market=args.market)
    else:
        raise SystemExit("Unsupported exchange")

    writer = CsvDailyRotator(outdir=args.outdir, prefix=args.prefix)

    try:
        for ev in stream_iter:
            writer.write_event(ev)
            if not args.quiet:
                print(fmt_line(ev))
    except KeyboardInterrupt:
        pass
    finally:
        writer.close()


if __name__ == "__main__":
    main()
