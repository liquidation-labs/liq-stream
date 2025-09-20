# writer_csv.py

import csv
import os
from datetime import datetime, timezone

SCHEMA = [
    "exchange","market","symbol","side","qty","price","notional",
    "ts_exch_ms","ts_ingest_ms","raw"
]

class CSVWriter:
    def __init__(self, outdir: str):
        self.outdir = outdir
        self._open_for_today()

    def _today_fname(self):
        d = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        return os.path.join(self.outdir, f"liquidations_{d}.csv")

    def _open_for_today(self):
        self.current_path = self._today_fname()
        self._f = open(self.current_path, "a", newline="", encoding="utf-8")
        self._w = csv.DictWriter(self._f, fieldnames=SCHEMA)
        if os.stat(self.current_path).st_size == 0:
            self._w.writeheader()

    def _rotate_if_needed(self):
        if self.current_path != self._today_fname():
            self._f.close()
            self._open_for_today()

    def write_row(self, row: dict):
        self._rotate_if_needed()
        # only keep known keys
        safe = {k: row.get(k, "") for k in SCHEMA}
        self._w.writerow(safe)
        # flush reasonably quickly for safety
        self._f.flush()
