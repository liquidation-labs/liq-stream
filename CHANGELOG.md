# Changelog

All notable changes to this project will be documented here.

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
