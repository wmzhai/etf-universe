# ETF Universe Design

**Date:** 2026-03-31

## Goal

Build `etf-universe` as an agent-agnostic Python package that fetches current holdings for a curated ETF universe and writes one parquet file plus one metadata sidecar file per ETF into a selected output directory.

The package is designed for direct terminal use and for agent runtimes that can invoke the same CLI. It does not depend on Codex-, Claude-, or repository-specific skill wrappers.

## Public Interface

The public interface is a flat CLI:

```bash
uv run etf-universe
uv run etf-universe list
uv run etf-universe fetch --symbols SPY,QQQ --output-dir ./data/universe/etf
```

Behavior:

- `uv run etf-universe` fetches all supported ETFs into `data/universe/etf`
- `uv run etf-universe list` prints one supported ETF symbol per line
- `uv run etf-universe fetch --symbols ...` fetches only the requested comma-separated ETF list
- Unknown ETF symbols are rejected before any network fetch begins

## Supported Universe

Version 1 ships a curated ETF registry in `src/etf_universe/registry.py`. The registry is the source of truth for supported symbols, issuers, providers, and source URLs.

The README must keep its supported ETF block synchronized with the registry.

## Output Contract

Each successful fetch writes:

- `{output_dir}/{SYMBOL}.parquet`
- `{output_dir}/{SYMBOL}.meta.json`

Parquet rows contain:

- `symbol`
- `name`
- `weight`

Metadata contains:

- `schemaVersion`
- `etfSymbol`
- `issuer`
- `provider`
- `asOfDate`
- `fetchedAt`
- `sourceUrl`
- `sourceFormat`
- `rowCount`
- `normalizedRowCount`
- `droppedRowCount`

## Data Rules

- Symbols are trimmed, uppercased, and normalized to dot form, for example `BRK/B` becomes `BRK.B`
- Cash placeholders, currency placeholders, malformed local-market tickers, and other non-holding rows are dropped locally before storage
- `asOfDate` and `fetchedAt` live in metadata, not parquet rows
- A fetch fails if an ETF normalizes down to zero usable holdings rows

## Symbol Validation

Version 1 validates candidate symbols with Alpaca `GET /v2/stocks/quotes/latest`.

Validation rules:

- Candidate symbols are collected across the full run, then deduplicated once
- Dot-form share classes such as `BRK.B` remain in dot form for both Alpaca requests and stored output
- Validation uses batches of up to 200 symbols
- Validation runs up to 8 batches concurrently
- If Alpaca returns `400 invalid symbol`, the reported symbol is removed and the remaining batch is retried
- Symbols missing from a successful `quotes` payload are treated as invalid
- Alpaca credentials are read from `.env` or process environment variables
- If credentials are missing, Alpaca validation is disabled and all locally eligible normalized symbols are kept

## Fetch Strategy

The implementation is registry-driven. Each ETF maps to a provider adapter and a fixed source URL.

Provider strategies:

- `ssga`: official XLSX holdings files
- `ishares`: official CSV holdings endpoints
- `vaneck`: HTML page fetch plus embedded dataset extraction
- `first_trust`: HTML holdings table parsing
- `invesco`: Playwright-assisted JSON discovery and retrieval

Concurrency model:

- Non-Invesco ETFs run concurrently with dedicated request sessions
- The current default is up to 16 concurrent non-browser ETF fetches
- Invesco ETFs run serially inside one browser session because they share one Playwright page
- Results are reassembled in requested ETF order before normalization and output

## Package Layout

```text
src/etf_universe/
  __init__.py
  cli.py
  contracts.py
  normalization.py
  registry.py
  runtime_logging.py
  storage.py
  validation.py
  providers/
    __init__.py
    base.py
    first_trust.py
    invesco.py
    ishares.py
    ssga.py
    vaneck.py
```

Module responsibilities:

- `cli.py`: argument parsing, orchestration, environment loading
- `contracts.py`: dataclasses and shared contracts
- `registry.py`: supported ETF registry and symbol parsing
- `normalization.py`: text cleanup, symbol normalization, row filtering, metadata shaping
- `validation.py`: Alpaca validation batching and retries
- `runtime_logging.py`: structured `stderr` logging and timing helpers
- `storage.py`: parquet and metadata writing
- `providers/*.py`: provider-specific fetch and parse logic

## Dependencies

Core runtime dependencies:

- `requests`
- `beautifulsoup4`
- `openpyxl`
- `playwright`
- `pyarrow`

The repository documents `uv` workflows only.

## Testing Strategy

Version 1 is covered by:

- registry tests
- normalization tests
- Alpaca validation tests
- provider parser tests
- CLI orchestration tests
- README and documentation consistency tests

## Limitations

- The ETF universe is curated; arbitrary ETF discovery is out of scope
- Each run exports only the latest provider snapshot
- Invesco fetches require Playwright and remain the main fetch-phase bottleneck
