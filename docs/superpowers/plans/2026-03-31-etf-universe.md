# ETF Universe Implementation Plan

**Status:** Implemented

## Objective

Maintain `etf-universe` as a standalone Python package managed with `uv`, with a flat CLI for listing supported ETFs and exporting normalized holdings snapshots for a curated ETF universe.

## Public CLI

Only the current flat command hierarchy is supported:

```bash
uv run etf-universe
uv run etf-universe list
uv run etf-universe fetch --symbols SPY,QQQ --output-dir ./data/universe/etf
```

Operational expectations:

- The bare command fetches the entire supported ETF universe into `data/universe/etf`
- `list` prints one symbol per line
- `fetch` requires `--symbols` and accepts an optional `--output-dir`
- No deprecated command aliases or compatibility layers are maintained

## Implemented Workstreams

- [x] Package bootstrap with `uv`, `pyproject.toml`, and a console entry point
- [x] Curated ETF registry in `src/etf_universe/registry.py`
- [x] Shared contracts in `src/etf_universe/contracts.py`
- [x] Row filtering and symbol normalization in `src/etf_universe/normalization.py`
- [x] Parquet and metadata output in `src/etf_universe/storage.py`
- [x] Structured runtime logging in `src/etf_universe/runtime_logging.py`
- [x] Provider adapters for SSGA, iShares, VanEck, First Trust, and Invesco
- [x] Alpaca-backed batched symbol validation in `src/etf_universe/validation.py`
- [x] CLI orchestration in `src/etf_universe/cli.py`
- [x] Test coverage for providers, CLI behavior, validation, config loading, and docs consistency

## Current Runtime Defaults

- Default output directory: `data/universe/etf`
- Non-Invesco fetch concurrency: `16`
- Alpaca validation batch size: `200`
- Alpaca validation concurrent batches: `8`
- HTTP timeout: `60` seconds

## Validation and Config Plan

Current validation behavior:

- Read `ALPACA_DATA_API_KEY`, `ALPACA_DATA_SECRET_KEY`, and optional `ALPACA_DATA_BASE_URL` from process environment or `.env`
- Normalize candidate symbols to uppercase dot form
- Deduplicate candidates across the full run before validation
- Validate with Alpaca `latest quotes`
- Retry batches after isolating `invalid symbol` failures
- Skip Alpaca validation entirely when credentials are absent, while still applying local symbol and row filters

Tracked config artifacts:

- `.env.sample` documents required variables without real credentials
- `.env` stays ignored and local-only

## Provider Execution Plan

- `ssga`: XLSX parsing
- `ishares`: CSV parsing
- `vaneck`: HTML plus embedded dataset extraction
- `first_trust`: holdings table parsing
- `invesco`: Playwright-assisted browser fetch

Execution model:

- Non-Invesco ETFs run concurrently with dedicated request sessions
- Invesco ETFs run serially inside one Playwright browser session
- Results are normalized and written in ETF request order

## Documentation Plan

Repository Markdown files must stay aligned with current implementation:

- `README.md` documents installation, CLI usage, output contract, validation behavior, and supported ETFs
- `AGENTS.md` records repository-specific workflow rules
- This implementation plan records the shipped execution model and maintenance defaults
- The design doc records the current architecture and constraints

Explicitly excluded from docs:

- deprecated CLI names
- deprecated output directories
- removed validation backends
- historical migration steps that no longer describe the shipped implementation

## Verification Plan

Primary verification command:

```bash
uv run pytest -v
```

Operational verification:

```bash
uv run etf-universe list
uv run etf-universe fetch --symbols SPY,QQQ --output-dir ./data/universe/etf
uv run etf-universe
```

## Maintenance Rules

- Keep the README supported ETF block synchronized with `src/etf_universe/registry.py`
- Update docs whenever CLI behavior, validation behavior, output paths, or provider strategy changes
- Do not add compatibility shims for removed commands or removed validation backends
