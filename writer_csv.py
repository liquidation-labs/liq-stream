# writer_csv.py
import csv
import os
from datetime import datetime, timezone
from typing import Dict, Optional

CSV_HEADERS = [
    "ts_iso","ts_ms","exchange","market","symbol","side","liq_side","price","qty","notional"
]


def _utc_date_str(ts_iso: str) -> str:
    # ts_iso like 2025-09-16T19:40:50.211000+00:00
    # We parse only date portion (first 10 chars)
    return ts_iso[:10]  # YYYY-MM-DD


class CsvDailyRotator:
    """
    Writes events to CSV files rotated by UTC date.
    Path pattern: {outdir}/{prefix}_{YYYY-MM-DD}.csv
    """
    def __init__(self, outdir: str = ".", prefix: str = "liquidations"):
        self.outdir = outdir
        self.prefix = prefix
        self._date_key: Optional[str] = None
        self._fh = None
        self._writer: Optional[csv.DictWriter] = None
        os.makedirs(self.outdir, exist_ok=True)

    def _open_for_date(self, date_key: str):
        if self._fh:
            self._fh.close()

        filename = f"{self.prefix}_{date_key}.csv"
        path = os.path.join(self.outdir, filename)
        file_exists = os.path.exists(path)

        self._fh = open(path, "a", newline="", encoding="utf-8")
        self._writer = csv.DictWriter(self._fh, fieldnames=CSV_HEADERS)

        if not file_exists or os.path.getsize(path) == 0:
            self._writer.writeheader()
            self._fh.flush()

        self._date_key = date_key

    def write_event(self, ev: Dict):
        date_key = _utc_date_str(ev["ts_iso"])
        if date_key != self._date_key:
            self._open_for_date(date_key)

        assert self._writer is not None
        row = {k: ev.get(k) for k in CSV_HEADERS}
        self._writer.writerow(row)
        self._fh.flush()

    def close(self):
        if self._fh:
            self._fh.close()
            self._fh = None
            self._writer = None
            self._date_key = None
