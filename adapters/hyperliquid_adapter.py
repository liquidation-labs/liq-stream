# adapters/hyperliquid_adapter.py

import asyncio
import io
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, Generator, Iterable, List, Optional, Tuple

def _now_ms() -> int:
    return int(time.time() * 1000)

def _to_ms(ts: Any) -> Optional[int]:
    """
    Convert various ts formats to milliseconds.
    Accepts: int/float seconds or ms; ISO strings; None.
    """
    if ts is None:
        return None
    # numeric?
    try:
        v = float(ts)
        # Heuristic: < 1e12 => seconds; >= 1e12 => ms
        return int(v * 1000) if v < 1e12 else int(v)
    except Exception:
        pass
    # string (ISO-ish)
    try:
        from datetime import datetime
        # allow "2025-09-23T12:34:56.789Z" or without Z
        s = str(ts).rstrip("Z")
        dt = datetime.fromisoformat(s)
        return int(dt.timestamp() * 1000)
    except Exception:
        return None

def _liq_side_from_kind(kind: str) -> Optional[str]:
    """
    'Long' => long positions liquidated
    'Short' => short positions liquidated
    """
    k = (kind or "").lower()
    if k == "long":
        return "long"
    if k == "short":
        return "short"
    return None

def _classify_liq_kind(dir_str: str, side: str) -> str:
    """
    Mirror your script: prefer textual hint in 'dir'; fall back to side A/B.
    """
    d = (dir_str or "").lower()
    s = (side or "").upper()
    if "close long" in d:
        return "Long"
    if "close short" in d:
        return "Short"
    return "Long" if s == "A" else ("Short" if s == "B" else "Unknown")

def _abs_float(x: Any) -> Optional[float]:
    try:
        return abs(float(x))
    except Exception:
        return None

def _iter_all_hour_files(root: Path) -> List[Path]:
    """
    Return all hour files sorted by day (asc) then hour (asc).
    Expects directory structure: root/YYYYMMDD/HH (files named '0'..'23')
    """
    files: List[Path] = []
    if not root.exists():
        return files
    # days like 20250923
    for d in sorted([p for p in root.iterdir() if p.is_dir() and p.name.isdigit()], key=lambda p: int(p.name)):
        # hours like 0..23 (files)
        hours = [f for f in d.iterdir() if f.is_file() and f.name.isdigit()]
        hours_sorted = sorted(hours, key=lambda p: int(p.name))
        files.extend(hours_sorted)
    return files

def _latest_hour_file(root: Path) -> Optional[Path]:
    files = _iter_all_hour_files(root)
    return files[-1] if files else None

def _open_follow(path: Path):
    f = open(path, "r", encoding="utf-8")
    f.seek(0, io.SEEK_END)
    ino = os.fstat(f.fileno()).st_ino
    pos = f.tell()
    buf = ""
    return f, ino, pos, buf

def _rotated(path: Path, ino: int, pos: int) -> bool:
    try:
        st = os.stat(path)
    except FileNotFoundError:
        return True
    return (st.st_ino != ino) or (st.st_size < pos)

class HyperliquidAdapter:
    """
    Reads Hyperliquid node fill logs produced by hl-visor (hourly rolling files) and emits
    normalized liquidation events to the provided writer (CSV/PG).

    Directory layout assumed:
      ~/hl/data/node_fills_streaming/hourly/YYYYMMDD/HH

    Each line is a JSON record:
      {
        "local_time": ..., "block_time": ..., "block_number": ...,
        "events": [
          [ "<taker_address>", { "coin": "...", "px": "...", "sz": "...", "dir": "...",
                                 "side": "A|B", "fee": "...", "feeToken": "...",
                                 "hash": "...", "tid": "...",
                                 "liquidation": { "liquidatedUser": "...", "markPx": "...", "method": "..." }
                               } ],
          ...
        ]
      }

    We keep only liquidation fills where taker == liquidation.liquidatedUser
    (i.e., the event is for the liquidated user themselves).
    """

    EXCHANGE = "hyperliquid"

    def __init__(
        self,
        writer,
        root_dir: Optional[str] = None,
        market: str = "usdc",
        min_abs_sz: float = 0.0,
        poll_sec: float = 0.15,
        rollover_check_sec: float = 1.0,
        catch_up: bool = True,  # process historical files before tailing
    ):
        self.writer = writer
        self.market = market  # "usdc" is accurate for HL perps
        self.root = Path(root_dir or (Path.home() / "hl" / "data" / "node_fills_streaming" / "hourly"))
        self.min_abs_sz = float(min_abs_sz)
        self.poll_sec = float(poll_sec)
        self.rollover_check_sec = float(rollover_check_sec)
        self.catch_up = bool(catch_up)

        # simple in-memory dedupe ring
        from collections import deque
        self._seen = set()
        self._ring = deque(maxlen=50_000)

    def _seen_key(self, e: Dict[str, Any]) -> str:
        return f"{e.get('tid')}|{e.get('liq_user')}|{e.get('coin')}"

    def _check_seen(self, k: str) -> bool:
        if k in self._seen:
            return True
        self._seen.add(k)
        self._ring.append(k)
        if len(self._ring) == self._ring.maxlen:
            old = self._ring.popleft()
            self._seen.discard(old)
        return False

    def _parse_line_liqs(self, line: str) -> List[Dict[str, Any]]:
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            return []
        out: List[Dict[str, Any]] = []
        lt = rec.get("local_time")
        bt = rec.get("block_time")
        bn = rec.get("block_number")
        for ev in rec.get("events", []):
            if not isinstance(ev, list) or len(ev) != 2:
                continue
            taker, fill = ev
            if not isinstance(fill, dict):
                continue
            liq = fill.get("liquidation")
            if not isinstance(liq, dict):
                continue

            liq_user = liq.get("liquidatedUser")
            # only take fills where taker == liquidated user
            if taker != liq_user:
                continue

            sz_abs = _abs_float(fill.get("sz"))
            if sz_abs is None or sz_abs < self.min_abs_sz:
                continue

            out.append({
                "local_time": lt,
                "block_time": bt,
                "block_number": bn,
                "coin": fill.get("coin"),
                "px": fill.get("px"),
                "sz": fill.get("sz"),
                "dir": fill.get("dir"),
                "side": fill.get("side"),
                "fee": fill.get("fee"),
                "feeToken": fill.get("feeToken"),
                "hash": fill.get("hash"),
                "tid": fill.get("tid"),
                "liq_user": liq_user,
                "liq_mark_px": liq.get("markPx"),
                "liq_method": liq.get("method"),
                "liq_kind": _classify_liq_kind(fill.get("dir"), fill.get("side")),
            })
        return out

    def _normalize_and_write(self, e: Dict[str, Any]):
        """
        Map HL liquidation fill -> unified schema row.
        """
        ts_ingest = _now_ms()
        # timestamps: prefer block_time
        ts_exch_ms = _to_ms(e.get("block_time")) or _to_ms(e.get("local_time"))

        # symbol: coin + "USDC" for HL perps
        coin = (e.get("coin") or "").upper()
        symbol = f"{coin}USDC" if coin else ""

        # price & qty
        price = None
        qty = None
        try:
            price = float(e.get("px") or 0.0)
        except Exception:
            price = None
        try:
            qty = _abs_float(e.get("sz"))
        except Exception:
            qty = None

        notional = (price * qty) if (price and qty) else None

        side_kind = e.get("liq_kind") or ""
        side = _liq_side_from_kind(side_kind)  # "long" | "short" | None

        raw = json.dumps(e, separators=(",", ":"))

        out = {
            "exchange": self.EXCHANGE,
            "market": self.market,        # "usdc"
            "symbol": symbol,
            "side": side,                 # "long" or "short"
            "qty": qty,
            "price": price,
            "notional": notional,
            "ts_exch_ms": ts_exch_ms,
            "ts_ingest_ms": ts_ingest,
            "raw": raw,
        }
        self.writer.write_row(out)

    def _process_file_full(self, path: Path):
        """
        Read a whole hour file from start, emit rows.
        """
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    if "liquidation" not in line:
                        continue
                    for ev in self._parse_line_liqs(line):
                        k = self._seen_key(ev)
                        if self._check_seen(k):
                            continue
                        self._normalize_and_write(ev)
        except Exception as e:
            print(f"[hyperliquid] error reading {path}: {e}")

    async def _tail_latest(self):
        """
        Tail the newest hour file, handle rollover to next hour automatically.
        """
        current = _latest_hour_file(self.root)
        while not current:
            print(f"[hyperliquid] waiting for hour file under {self.root} …")
            await asyncio.sleep(1.0)
            current = _latest_hour_file(self.root)

        print(f"[hyperliquid] tailing {current.parent.name}/{current.name} -> {current}")
        f, ino, pos, buf = _open_follow(current)
        last_roll = time.time()
        hb = 0.0

        try:
            while True:
                # check rollover to a newer hour file
                if time.time() - last_roll >= self.rollover_check_sec:
                    latest = _latest_hour_file(self.root)
                    if latest and latest != current:
                        try:
                            f.close()
                        except Exception:
                            pass
                        current = latest
                        print(f"[hyperliquid] rollover -> {current.parent.name}/{current.name}")
                        f, ino, pos, buf = _open_follow(current)
                    last_roll = time.time()

                # handle log rotation/truncate
                if _rotated(current, ino, pos):
                    try:
                        f.close()
                    except Exception:
                        pass
                    f, ino, pos, buf = _open_follow(current)

                chunk = f.read()
                if not chunk:
                    now = time.time()
                    if now - hb >= 30:
                        print(f"[hyperliquid] heartbeat file={current}")
                        hb = now
                    await asyncio.sleep(self.poll_sec)
                    continue

                pos = f.tell()
                buf += chunk

                while "\n" in buf:
                    line, buf = buf.split("\n", 1)
                    if not line.strip() or "liquidation" not in line:
                        continue
                    for ev in self._parse_line_liqs(line):
                        k = self._seen_key(ev)
                        if self._check_seen(k):
                            continue
                        self._normalize_and_write(ev)
        finally:
            try:
                f.close()
            except Exception:
                pass

    async def run(self):
        # 1) optional backfill of all existing files
        if self.catch_up:
            files = _iter_all_hour_files(self.root)
            if files:
                print(f"[hyperliquid] backfilling {len(files)} hour files from {self.root}")
            for p in files:
                self._process_file_full(p)

        # 2) tail latest live
        await self._tail_latest()
