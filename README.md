[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![Status](https://img.shields.io/badge/status-beta-green)

Stream crypto **liquidation events** from multiple exchanges into clean CSVs or a Postgres database, with unified schema and daily file rotation.  
Supports **Binance (USDT-M & COIN-M)**, **Bybit (USDT-M & Inverse/COIN-M)**, **OKX (USDT-M & COIN-M)**, **Aster (USDT-M only)**, and **Hyperliquid (Perps, USDC – via local node)**.


## ✨ Features
- 🔥 Run **all exchanges + markets at once** with one command (`--all`)
- 🎯 Choose a custom set of streams with `--streams`
- 💾 Write to **CSV, Postgres, or both**
- 🗂 Writes **daily CSVs** partitioned by exchange/market
- 🛢 Batched Postgres inserts with `asyncpg`
- 📊 Unified schema across exchanges
- 🎨 Color-coded console prints (long liqs = red, short liqs = green)
- 🔌 Adapter pattern → new exchanges easy to add

## 🚀 Quickstart
```bash
pip install -r requirements.txt

# Run everything (Binance + Bybit + OKX + Aster) → CSVs
python -m stream --all --outdir-root data

# Run everything → Postgres
python -m stream --all --sink pg --pg-dsn "postgresql://user:pw@localhost:5432/liqdb"

# Run everything → CSV + Postgres
python -m stream --all --sink both --outdir-root data --pg-dsn "postgresql://user:pw@localhost:5432/liqdb"
```

## 🎮 Usage Examples
```bash
# Run a custom set of streams
python -m stream --streams binance:usdt,bybit:coin,okx:usdt,aster:usdt --outdir-root data

# Run a single stream
python -m stream --exchange aster --market usdt --outdir data/aster_usdt

# Run Hyperliquid only (must have node running and writing fills)
python -m stream --exchange hyperliquid --market usdc \
  --hl-root ~/hl/data/node_fills_streaming/hourly \
  --outdir data/hl_usdc

# Extra options
Print only (no CSV/DB):
python -m stream --all --no-write

Disable colors:
python -m stream --all --no-color

Control Bybit subscription chunk size:
python -m stream --streams bybit:usdt,bybit:coin --subscribe-chunk 50

Change Postgres batch + interval:
python -m stream --all --sink pg --pg-dsn "$PG_DSN" --pg-batch 50 --pg-interval 0.5
```
## 🌀 Hyperliquid Setup
Unlike other adapters, **Hyperliquid has no API/WebSocket feed**. You must:

1. **Run a node (hl-visor)** with fill logging enabled:
   ```bash
   ./hl-visor run-non-validator --write-fills --stream-with-block-info
    ```

2. **Verify logs** appear at:
    ```
    ~/hl/data/node_fills_streaming/hourly
    ```

3. **Point the adapter** at that root dir using --hl-root (or leave default ~/hl/data/node_fills_streaming/hourly).

More info at: https://github.com/hyperliquid-dex/node

## 📁 Unified Schema
Each row has:

- **exchange**: binance | bybit | okx | aster | hyperliquid
- **market**: usdt | coin (Aster = usdt only) | usdc (Hyperliquid only)
- **symbol**: trading pair symbol (Hyperliquid symbols normalized as COINUSDC)
- **side**: long = long positions liquidated (forced sell), short = short positions liquidated (forced buy)
- **qty**: contracts/amount liquidated
- **price**: execution price
- **notional**: price × qty
- **ts_exch_ms**: exchange timestamp (ms)
- **ts_ingest_ms**: local ingest timestamp (ms)
- **raw**: compact JSON from the exchange

Files rotate daily (UTC).

## 📊 Schema diagram
```mermaid
erDiagram
    liquidations {
        text exchange
        text market
        text symbol
        text side
        double qty
        double price
        double notional
        bigint ts_exch_ms
        bigint ts_ingest_ms
        jsonb raw
    }
```

## 📷 Data Flow

```mermaid
flowchart LR
  A["Binance !forceOrder@arr"] --> N["Normalizer"]
  B["Bybit allLiquidation.&lt;SYMBOL&gt;"] --> N
  C["OKX liquidation-orders"] --> N
  D["Aster !forceOrder@arr"] --> N
  E["Hyperliquid node logs (hourly)"] --> N
  N --> W["WriterShim"]
  W --> F["CSV (daily rotate)"]
  W --> P["Postgres (batch inserts)"]
  W --> T["Terminal prints (color-coded)"]
```

## 📌 Notes
- **Binance**: uses all-symbols liquidation feed (!forceOrder@arr), USDT-M via fstream, COIN-M via dstream.
- **Bybit**: auto-discovers all symbols via REST (linear for USDT-M, inverse for COIN-M), subscribes to each with allLiquidation.<SYMBOL>.
- **OKX**: single subscription to liquidation-orders (instType=SWAP), filters *-USDT/USDC-SWAP vs *-USD-SWAP.
- **Aster**: single all-symbols liquidation feed (!forceOrder@arr), USDT-margined only.
- **Hyperliquid**: requires local node with --write-fills; adapter tails hourly JSON files instead of connecting to an API.
