# Liquidation Labs — liq-stream

[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![Status](https://img.shields.io/badge/status-beta-green)

Stream crypto **liquidation events** from multiple exchanges into clean CSVs, with unified schema and daily file rotation.  
Supports **Binance (USDT-M & COIN-M)**, **Bybit (USDT-M & Inverse/COIN-M)**, and **OKX (USDT-M & COIN-M)**.

## ✨ Features

- 🔥 Run **all exchanges + markets at once** with one command (`--all`)
- 🎯 Choose a custom set of streams with `--streams`
- 💾 Writes **daily CSVs** partitioned by exchange/market
- 📊 Unified schema across exchanges
- 🎨 Color-coded console prints (long liqs = red, short liqs = green)
- 🔌 Adapter pattern → new exchanges easy to add

## 🚀 Quickstart

```bash
pip install -r requirements.txt

# Run everything (Binance + Bybit + OKX, USDT & COIN)
python -m stream --all --outdir-root data
```

## 🎮 Usage

### Run all streams

```bash
python -m stream --all --outdir-root data
```

This creates:

```
data/
  binance_usdt/liquidations_YYYY-MM-DD.csv
  binance_coin/liquidations_YYYY-MM-DD.csv
  bybit_usdt/liquidations_YYYY-MM-DD.csv
  bybit_coin/liquidations_YYYY-MM-DD.csv
  okx_usdt/liquidations_YYYY-MM-DD.csv
  okx_coin/liquidations_YYYY-MM-DD.csv
```

### Run a custom set of streams

```bash
python -m stream --streams binance:usdt,bybit:coin,okx:usdt --outdir-root data
```

### Run a single stream (backwards compatible)

```bash
python -m stream --exchange binance --market usdt --outdir data/binance_usdt
```

### Extra options

- Print only (no CSV):

  ```bash
  python -m stream --all --no-write
  ```

- Disable colors:

  ```bash
  python -m stream --all --no-color
  ```

- Control Bybit subscription chunk size:

  ```bash
  python -m stream --streams bybit:usdt,bybit:coin --subscribe-chunk 50
  ```

## 📁 Unified Schema

Each CSV row has:

```
exchange, market, symbol, side, qty, price, notional, ts_exch_ms, ts_ingest_ms, raw
```

- **exchange**: `binance` | `bybit` | `okx`  
- **market**: `usdt` | `coin`  
- **side**:  
  - `long` = long positions liquidated (forced sell)  
  - `short` = short positions liquidated (forced buy)  
- **ts_exch_ms**: exchange timestamp (ms)  
- **ts_ingest_ms**: local ingest timestamp (ms)  
- **raw**: compact JSON from the exchange  

Files rotate daily (UTC).

## 📷 Visual

```mermaid
flowchart LR
  A[Binance !forceOrder@arr] --> N[Normalizer]
  B[Bybit v5 liquidation.SYMBOL] --> N
  C[OKX v5 liquidation-orders] --> N
  N --> W[WriterShim]
  W --> F[CSV (daily rotate)]
  W --> T[Terminal Prints (color-coded)]
```

## 📌 Notes

- **Binance**: uses all-symbols liquidation feed (`!forceOrder@arr`), USDT-M via `fstream`, COIN-M via `dstream`.  
- **Bybit**: auto-discovers all symbols via REST (`linear` for USDT-M, `inverse` for COIN-M), subscribes to each.  
- **OKX**: single subscription to `liquidation-orders` (`instType=SWAP`), filters `*-USDT/USDC-SWAP` vs `*-USD-SWAP`.