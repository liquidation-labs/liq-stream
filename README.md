# Liquidation Labs — Binance Liquidations → CSV

Stream Binance Futures liquidation events into a CSV.  
Supports USDT-M (`fstream`) and COIN-M (`dstream`), optional symbol filter, auto-reconnect, and graceful shutdown.

---

## 🚀 Quickstart

```bash
# 1. Create and activate a virtual environment
python -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run (USDT-M all pairs → liquidations.csv)
python liq_stream.py --market usdt

# Example: COIN-M only
python liq_stream.py --market coin --outfile coinm_liqs.csv

# Example: Single symbol only
python liq_stream.py --symbol BTCUSDT
