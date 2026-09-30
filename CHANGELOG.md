# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Added `VTI` (Vanguard Total Stock Market ETF) with a Vanguard fund-profile adapter.
- Added `IWC` (iShares Micro-Cap ETF) on the existing iShares adapter.

## [0.1.2] - 2026-09-20

### Added

- Added `SPMO` (Invesco S&P 500 Momentum ETF) to the curated registry.

## [0.1.1] - 2026-08-24

### Removed

- Dropped `GDX` from the curated ETF registry.

## [0.1.0] - 2026-07-14

### Added

- A flat CLI for listing supported ETFs and exporting either the complete
  universe or a requested symbol subset.
- A curated 37-ETF registry with provider adapters for ARK, First Trust,
  Invesco, iShares, SSGA, and VanEck.
- Normalized Zstandard-compressed Parquet holdings and JSON metadata sidecars
  with best-effort fund profile data.
- Optional concurrent Alpaca symbol validation, concurrent non-browser
  fetching, structured runtime logs, and Playwright-backed Invesco support.
- Maintainer-facing architecture documentation covering the fetch pipeline,
  data invariants, provider boundaries, and release confidence checks.

### Fixed

- Updated iShares holdings downloads to use the current official BlackRock
  document API while retaining product-page profile enrichment.

[Unreleased]: https://github.com/wmzhai/etf-universe/compare/v0.1.2...HEAD
[0.1.2]: https://github.com/wmzhai/etf-universe/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/wmzhai/etf-universe/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/wmzhai/etf-universe/releases/tag/v0.1.0
