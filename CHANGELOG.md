# Changelog

All notable changes to this project will be documented here.

### 📌 Changelog entry

## v0.6.0 (2025-09-27)
### Added
- **Aster adapter** (USDT-M only) with normalized liquidation stream.
- **Hyperliquid adapter** (USDC): tails local node `fills_streaming` logs and normalizes liquidation fills.
- `--all` now includes `aster:usdt` and `hyperliquid:usdc`.
- Simple terminal dashboards:
  - `liq_simple.sh` + `liq_simple.awk` — counts by exchange (windowed or all-time).
  - `hyperliquid_6h.sh` + `hyperliquid_6h.awk` — last 6h activity (ASCII-safe).
### Changed
- `adapters/__init__.py`: wired `AsterAdapter` and `HyperliquidAdapter` into `get_adapter`.
- `stream.py`: constructor compatibility (Aster), USDC market handling (Hyperliquid), and `--all` set updated.
- README: documented Aster + Hyperliquid, added troubleshooting.
### Notes
- **Schema unchanged** (still `exchange, market, symbol, side, qty, price, notional, ts_exch_ms, ts_ingest_ms, raw`).
- New markets: `market` can now be `usdc` (Hyperliquid).

## v0.5.0 (2025-09-24)
### Added
- **Aster adapter** (USDT-M only) with unified schema
- `--exchange aster` and `--streams ... aster:usdt`
- Included in `--all` mode
- Docs updated (README + diagrams)

### Fixed
- Stream runner now passes `market` argument safely to Aster

## v0.4.0 (2025-09-22)
### Added
- Postgres sink (`--sink pg`) with batch inserts via `asyncpg`
- Combined CSV + Postgres sink (`--sink both`)
- Configurable batch size and flush interval (`--pg-batch`, `--pg-interval`)
- New table schema: unified across exchanges (`exchange, market, symbol, side, qty, price, notional, ts_exch_ms, ts_ingest_ms, raw`)
- Docs updated with usage and diagrams

### Changed
- Bybit adapter upgraded to `allLiquidation.<SYMBOL>` channel
- Writer architecture refactored for multiple sinks


## v0.3.0 (2025-09-20)
- NEW: Multi-stream orchestrator (--all, --streams, --outdir-root)
- NEW: Color-coded terminal prints via WriterShim
- NEW: Bybit & OKX adapters (unified schema)
- REFACTOR: Binance adapter switched to async, unified schema

## v0.2.0 - 2025-09-18
- Refactor to adapter pattern (Binance)
- Added daily CSV rotation by UTC date
- Cleaner console output formatting

## v0.1.0 - 2025-09-16
- Initial release: Binance liquidation stream → single CSV
