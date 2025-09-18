# Liquidation Labs – liq-stream

Stream crypto liquidation events into CSV files.  
Currently supports **Binance (USDT-M & COIN-M)** with daily CSV rotation.  
Adapters for Bybit/OKX coming soon.

---

## Features
- Streams **all pairs** (USDT-M or COIN-M).
- Writes **daily CSVs** (e.g. `data/liquidations_2025-09-17.csv`).
- Unified schema across exchanges.
- Clear console output for easy monitoring.
- Refactored adapter pattern → new exchanges easy to plug in.

---

## Quickstart
```bash
git clone https://github.com/liquidation-labs/liq-stream.git
cd liq-stream
pip install -r requirements.txt

# Run Binance USDT-M
python stream.py --exchange binance --market usdt --outdir data
