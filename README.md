# etf-universe

Curated ETF holdings fetcher and exporter.

## Installation

1. `uv sync` to install dependencies defined in `pyproject.toml`.
2. Copy `.env.sample` to `.env` and fill in `ALPACA_DATA_API_KEY` plus `ALPACA_DATA_SECRET_KEY` if you want Alpaca-backed symbol validation. `ALPACA_DATA_BASE_URL` is optional and defaults to `https://data.alpaca.markets`.
3. If you intend to fetch Invesco ETFs, run `uv run playwright install chromium` so the headless browser is available when Playwright is launched.

## CLI examples

```sh
uv run etf-universe
uv run etf-universe --output-dir ~/data/universe/etf
uv run etf-universe list
uv run etf-universe fetch --symbols SPY,XLK --output-dir ~/data/universe/etf
```

The bare command runs a full fetch for every supported ETF. The default output directory is `~/data/universe/etf`.
The fetch command writes each ETF’s normalized holdings to `<output-dir>/<SYMBOL>.parquet` by default and a companion `<SYMBOL>.meta.json`.
Fetch commands also emit runtime logs to `stderr`, including upstream request URLs, response statuses, per-stage elapsed times, and total elapsed time.

## Supported ETFs

<!-- supported-etfs:start -->
`DIA`
`FDN`
`GDX`
`IGV`
`IHI`
`ITA`
`IWM`
`IYT`
`KBE`
`KRE`
`OIH`
`QQEW`
`QQQ`
`RSP`
`SMH`
`SPY`
`XBI`
`XLB`
`XLC`
`XLE`
`XLF`
`XLI`
`XLK`
`XLP`
`XLRE`
`XLU`
`XLV`
`XLY`
`XOP`
`XRT`
<!-- supported-etfs:end -->

## Output format

Each successful fetch produces:

- `<SYMBOL>.parquet`: normalized holdings rows with `symbol`, `name`, and `weight`.
- `<SYMBOL>.meta.json`: metadata documenting the snapshot ({schemaVersion, etfSymbol, issuer, provider, asOfDate, fetchedAt, sourceUrl, sourceFormat, rowCount, normalizedRowCount, droppedRowCount}).

All files land under the directory passed to `--output-dir` (defaults to `~/data/universe/etf`).

## Symbol validation

Symbols are normalized to upper-case with dots (e.g., `BRK.B`) and validated with Alpaca `latest quotes`. Dot-form share classes remain in dot form for both requests and stored output. When multiple ETFs are fetched together, the CLI first builds the full candidate-symbol universe, deduplicates it once, and then validates it in concurrent batches of up to 200 tickers, with up to 8 batches in flight at a time. Alpaca `400 invalid symbol` responses are reduced by removing the reported symbol and retrying the remaining batch. Symbols missing from a successful `quotes` payload are treated as invalid and dropped from the final holdings output. If Alpaca credentials are not configured, the validator is disabled and all locally eligible normalized symbols are kept.

## Development

`uv run pytest -v`

## Limitations

- The ETF roster is the curated `ETF_SPECS` dictionary in `src/etf_universe/registry.py`; please align any README updates with that source.
- Each fetch delivers only the latest snapshot published by the provider; historical snapshots must be stored externally.
- Invesco-backed ETFs require the Chromium browser because their holdings data is discovered in a Playwright browser context rather than a static CSV or XLSX endpoint.
- Non-Invesco ETFs are fetched concurrently, but Invesco ETFs still run serially inside one browser session.
