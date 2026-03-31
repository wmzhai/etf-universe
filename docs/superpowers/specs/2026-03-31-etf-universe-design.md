# ETF Universe Design

**Date:** 2026-03-31

## Goal

Build an agent-agnostic Python package named `etf-universe` that fetches current holdings for a curated subset of supported ETF symbols and writes one parquet file plus one metadata sidecar file per ETF into a user-selected output directory.

The package must be usable directly from the terminal and from any agent runtime through the same CLI. It must not depend on Codex-, Claude-, or platform-specific skill infrastructure.

## Naming

- Repository name: `etf-universe`
- Python distribution name: `etf-universe`
- Python import package: `etf_universe`
- CLI command: `etf-universe`

## Scope

### In Scope

- A standalone Python package managed and executed with `uv`
- A curated ETF registry maintained in source control
- Fetching holdings for a user-provided comma-separated ETF symbol list
- Provider-specific fetchers for the supported ETF subset
- Normalizing holdings into a shared parquet schema
- Writing one sidecar metadata JSON file per ETF
- A CLI for listing supported ETFs and fetching holdings
- English-only project docs and commit messages

### Out of Scope

- Arbitrary ETF discovery from unknown symbols
- Automatic issuer or product URL discovery
- A platform-specific skill wrapper
- A web UI or service API
- Historical holdings versioning in v1
- Persisting issuer raw payloads as the primary output contract

## Product Positioning

`etf-universe` is a tool-first package. The CLI is the stable public interface. Agents are expected to call the CLI rather than reimplement holdings logic or depend on repository-specific prompt instructions.

This keeps the package portable across Codex, Claude, direct terminal use, CI jobs, and future orchestration layers.

## Supported ETFs

Version 1 supports a curated subset copied from the current internal `wyckoff` implementation:

- `SPY`, `QQQ`, `DIA`, `IWM`
- `XLK`, `XLF`, `XLE`, `XLV`, `XLY`, `XLP`, `XLI`, `XLB`, `XLU`, `XLRE`, `XLC`
- `SMH`, `IGV`, `KRE`, `KBE`, `XOP`, `OIH`, `XBI`, `IHI`, `XRT`, `ITA`, `IYT`, `GDX`, `FDN`
- `RSP`, `QQEW`

The registry is the single source of truth for supported symbols. The README must list the supported ETFs, but that list must stay aligned with the registry through either generation or a consistency test.

## Runtime Model

The package exposes a single CLI with subcommands:

```bash
uv run etf-universe holdings list-supported
uv run etf-universe holdings fetch --symbols SPY,QQQ --output-dir ./data/etf-holdings
```

### Command: `holdings list-supported`

Purpose:

- Print the supported ETF symbol list for humans and agents

Initial behavior:

- Plain text output by default
- Optional machine-friendly output can be added later

### Command: `holdings fetch`

Purpose:

- Fetch holdings for a comma-separated list of supported ETF symbols

Arguments:

- `--symbols`: required comma-separated ETF list
- `--output-dir`: required or defaulted path for output files

Behavior:

- Reject unknown ETF symbols before any network work begins
- Fetch all requested ETFs
- Normalize holdings rows into a shared contract
- Validate candidate equity symbols with batched `yfinance` downloads
- Write one parquet file and one metadata file per ETF
- Print one summary line per ETF

## Output Contract

### Parquet Schema

- `symbol`
- `name`
- `weight`

### Metadata Schema

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

### File Layout

For a requested output directory:

- `{output_dir}/{TICKER}.parquet`
- `{output_dir}/{TICKER}.meta.json`

Version 1 only keeps the latest snapshot per ETF in the selected output directory.

## Data Rules

- `symbol` must be trimmed and uppercased
- Share-class separators like `BRK/B` and `BF/B` must normalize to dot form such as `BRK.B` and `BF.B`
- Rows with missing or obviously invalid symbols must be dropped before writing
- Cash placeholders, page noise, and malformed local-market symbols must be dropped
- `asOfDate` and `fetchedAt` belong in file-level metadata, not repeated in parquet rows

## Symbol Validation

Version 1 uses batched `yfinance` downloads instead of Alpaca credentials:

- Filter candidate symbols locally by allowed format
- Deduplicate symbols across all fetched ETFs in the current run
- Convert dot-form share classes such as `BRK.B` to Yahoo-compatible dash form such as `BRK-B` only for the remote validation request
- Validate candidates in batches with `yfinance.download(...)`
- Use `period="5d"` and `interval="1d"` to reduce false negatives from single-day trading gaps
- Treat a symbol as valid only when its per-symbol OHLCV frame is not empty, contains at least one non-all-`NaN` OHLCV row, and has at least one valid `Close` or `Volume` value
- Treat symbols with all-`NaN` OHLCV results as invalid
- Convert back to the original normalized dot form for internal storage and output
- Execute validation in bounded batches to avoid rate limiting

Validation in v1 does not require API credentials or symbol-validation environment variables.

## Provider Strategy

The architecture remains registry-driven. Each supported ETF maps to an issuer/provider pair and a known source URL.

### SSGA / SPDR

- Source format: official XLSX
- ETFs: `SPY`, `DIA`, `XLK`, `XLF`, `XLE`, `XLV`, `XLY`, `XLP`, `XLI`, `XLB`, `XLU`, `XLRE`, `XLC`, `KRE`, `KBE`, `XOP`, `XBI`, `XRT`

### iShares

- Source format: official holdings CSV
- ETFs: `IWM`, `IGV`, `IHI`, `ITA`, `IYT`
- Product URLs must be maintained explicitly, not inferred from ticker alone

### VanEck

- Source format: holdings page HTML leading to an embedded dataset JSON URL
- ETFs: `SMH`, `OIH`, `GDX`

### First Trust

- Source format: official holdings HTML table
- ETFs: `FDN`, `QQEW`
- Parsing must target the actual holdings table only

### Invesco

- Source format: browser-assisted JSON retrieval
- ETFs: `QQQ`, `RSP`
- Use Playwright as the browser automation fallback path

## Architecture

The package should be split into small, explicit modules instead of one large script.

### Package Layout

```text
src/etf_universe/
  __init__.py
  cli.py
  contracts.py
  registry.py
  normalization.py
  storage.py
  validation.py
  providers/
    __init__.py
    ssga.py
    ishares.py
    vaneck.py
    first_trust.py
    invesco.py
```

### Module Responsibilities

- `contracts.py`
  - Dataclasses and shared type definitions
- `registry.py`
  - Supported ETF registry and lookup helpers
- `normalization.py`
  - Symbol cleanup, row cleanup, and metadata shaping
- `storage.py`
  - Parquet and metadata writing
- `validation.py`
  - Batched `yfinance` symbol validation
- `providers/*.py`
  - Provider-specific HTTP or browser fetch logic
- `cli.py`
  - Argument parsing and orchestration only

This design keeps provider parsing isolated, keeps the CLI thin, and makes testing much easier than the current single-file internal script.

## Dependency Model

Use `uv` for environment management, dependency installation, command execution, and test execution.

Core dependencies:

- `requests`
- `yfinance`
- `beautifulsoup4`
- `openpyxl`
- `pyarrow`
- `playwright`

The repository should document only `uv` workflows and should not present `pip` as the primary path.

## README Requirements

The README must be in English and include:

- A short overview of the package
- The supported ETF list
- Installation with `uv`
- CLI examples
- Output schema
- Validation behavior and browser/runtime requirements
- Provider notes and current limitations

## Testing Strategy

Version 1 should add much broader tests than the internal script currently has.

Required test categories:

- Registry tests
- Normalization tests
- yfinance validation tests
- Provider parser tests
- CLI tests

Provider tests should focus on parser behavior with representative payloads or page fragments, so failures are easy to isolate when upstream issuer pages change.

## Migration from the Internal Script

The internal `wyckoff` script is the source material for v1 behavior, not the final structure.

Migration goals:

- Preserve supported ETFs and provider routing
- Preserve the current output contract
- Preserve the current symbol-validation intent while removing the Alpaca credential dependency
- Replace the monolithic script layout with a reusable package layout
- Remove direct coupling to `wyckoff` language and repository assumptions

## Future Expansion

Possible future extensions after v1:

- Machine-readable `list-supported` output
- Provider registry generation helpers
- Additional ETF issuers
- Additional output formats
- Snapshot versioning or dated output roots
- A Python API layer for embedding without shelling out

Those are explicitly deferred until the curated-subset CLI works end to end.
