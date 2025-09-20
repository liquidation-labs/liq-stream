# Changelog

All notable changes to this project will be documented here.

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
