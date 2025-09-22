# writer_pg.py
import asyncio
import json
import time
from typing import Optional, Sequence, Dict, Any

import asyncpg


SCHEMA_COLS = [
    "exchange",        # text
    "market",          # text
    "symbol",          # text
    "side",            # text (long|short)
    "qty",             # double precision
    "price",           # double precision
    "notional",        # double precision
    "ts_exch_ms",      # bigint
    "ts_ingest_ms",    # bigint
    "raw",             # jsonb (stored as JSON text)
]

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS {table_name} (
    exchange     TEXT NOT NULL,
    market       TEXT NOT NULL,
    symbol       TEXT NOT NULL,
    side         TEXT,
    qty          DOUBLE PRECISION,
    price        DOUBLE PRECISION,
    notional     DOUBLE PRECISION,
    ts_exch_ms   BIGINT,
    ts_ingest_ms BIGINT,
    raw          JSONB
);
"""

CREATE_INDEX_SQL = """
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = '{idx_time}' AND n.nspname = COALESCE(split_part('{table_name}', '.', 1), 'public')
    ) THEN
        EXECUTE 'CREATE INDEX {idx_time} ON {table_name} (ts_exch_ms)';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = '{idx_sym_time}' AND n.nspname = COALESCE(split_part('{table_name}', '.', 1), 'public')
    ) THEN
        EXECUTE 'CREATE INDEX {idx_sym_time} ON {table_name} (exchange, market, symbol, ts_exch_ms)';
    END IF;
END$$;
"""

INSERT_SQL = """
INSERT INTO {table_name} ({cols})
VALUES ({placeholders})
"""

def _now_ms() -> int:
    return int(time.time() * 1000)


class PostgresWriter:
    """
    Async, batched Postgres writer using asyncpg.
    - Queue + background task to batch inserts
    - Shared across all streams
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        table_name: str = "public.liquidations",
        batch_size: int = 500,
        flush_interval: float = 1.0,
        create_table: bool = True,
        create_indexes: bool = True,
    ):
        self.pool = pool
        self.table_name = table_name
        self.batch_size = max(1, batch_size)
        self.flush_interval = max(0.05, float(flush_interval))
        self.create_table = create_table
        self.create_indexes = create_indexes

        self._queue: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue(maxsize=50_000)
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

    @classmethod
    async def create(
        cls,
        dsn: str,
        table_name: str = "public.liquidations",
        batch_size: int = 500,
        flush_interval: float = 1.0,
        create_table: bool = True,
        create_indexes: bool = True,
        min_size: int = 1,
        max_size: int = 10,
    ) -> "PostgresWriter":
        pool = await asyncpg.create_pool(dsn=dsn, min_size=min_size, max_size=max_size)
        self = cls(
            pool=pool,
            table_name=table_name,
            batch_size=batch_size,
            flush_interval=flush_interval,
            create_table=create_table,
            create_indexes=create_indexes,
        )
        if create_table:
            await self._ensure_table()
        if create_indexes:
            await self._ensure_indexes()
        self._task = asyncio.create_task(self._run())
        return self

    async def _ensure_table(self):
        sql = CREATE_TABLE_SQL.format(table_name=self.table_name)
        async with self.pool.acquire() as conn:
            await conn.execute(sql)

    async def _ensure_indexes(self):
        idx_time = (self.table_name.split(".")[-1]) + "_ts_idx"
        idx_sym_time = (self.table_name.split(".")[-1]) + "_sym_ts_idx"
        sql = CREATE_INDEX_SQL.format(
            table_name=self.table_name,
            idx_time=idx_time,
            idx_sym_time=idx_sym_time,
        )
        async with self.pool.acquire() as conn:
            await conn.execute(sql)

    def write_row(self, row: Dict[str, Any]):
        """
        Adapters call this synchronously. We enqueue for async batch writing.
        """
        try:
            # Ensure required cols exist; fill defaults
            safe = {k: row.get(k) for k in SCHEMA_COLS}
            if safe["ts_ingest_ms"] is None:
                safe["ts_ingest_ms"] = _now_ms()

            # Always ensure raw is JSON string
            raw = safe.get("raw")
            if isinstance(raw, dict):
                safe["raw"] = json.dumps(raw, separators=(",", ":"))
            elif not isinstance(raw, str):
                try:
                    safe["raw"] = json.dumps(raw, separators=(",", ":"))
                except Exception:
                    safe["raw"] = "{}"

            self._queue.put_nowait(safe)
        except asyncio.QueueFull:
            # Drop oldest if queue is full
            try:
                _ = self._queue.get_nowait()
                self._queue.put_nowait(safe)
            except Exception:
                pass

    async def aclose(self):
        self._stop.set()
        if self._task:
            await self._task
        await self.pool.close()

    async def _run(self):
        """
        Background flush loop:
        - Pull items from queue
        - Batch INSERT at intervals or when batch is full
        """
        cols = ", ".join(SCHEMA_COLS)
        placeholders = ", ".join(f"${i+1}" for i in range(len(SCHEMA_COLS)))
        sql = INSERT_SQL.format(table_name=self.table_name, cols=cols, placeholders=placeholders)

        batch: list[Sequence[Any]] = []
        last_flush = time.perf_counter()

        while not (self._stop.is_set() and self._queue.empty()):
            timeout = self.flush_interval
            try:
                item = await asyncio.wait_for(self._queue.get(), timeout=timeout)
                batch.append([
                    item["exchange"],
                    item["market"],
                    item["symbol"],
                    item["side"],
                    item["qty"],
                    item["price"],
                    item["notional"],
                    item["ts_exch_ms"],
                    item["ts_ingest_ms"],
                    item["raw"],  # always JSON string now
                ])
            except asyncio.TimeoutError:
                pass

            now = time.perf_counter()
            if batch and (len(batch) >= self.batch_size or (now - last_flush) >= self.flush_interval):
                try:
                    async with self.pool.acquire() as conn:
                        await conn.executemany(sql, batch)
                except Exception as e:
                    print(f"[postgres] insert error ({len(batch)} rows): {e}")
                finally:
                    batch.clear()
                    last_flush = now

        # final flush
        if batch:
            try:
                async with self.pool.acquire() as conn:
                    await conn.executemany(sql, batch)
                print(f"[postgres] inserted {len(batch)} rows into {self.table_name}")
            except Exception as e:
                print(f"[postgres] final insert error ({len(batch)} rows): {e}")
