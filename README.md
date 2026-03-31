# etf-universe

Curated ETF holdings fetcher and exporter.

## Installation

1. `uv sync` to install dependencies defined in `pyproject.toml`.
2. If you intend to fetch Invesco ETFs, run `uv run playwright install chromium` so the headless browser is available when Playwright is launched.

## CLI examples

```sh
uv run etf-universe holdings list-supported
uv run etf-universe holdings fetch --symbols SPY,XLK
```

The fetch command writes each ETF’s normalized holdings to `data/etf-holdings/<SYMBOL>.parquet` by default and a companion `<SYMBOL>.meta.json`.

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

All files land under the directory passed to `--output-dir` (defaults to `data/etf-holdings`).

## Symbol validation

Symbols are normalized to upper-case with dots (e.g., `BRK.B`) and validated with `yfinance` over a five-day window at a one-day cadence. Validation makes HTTPS calls that convert dots to dashes (Yahoo’s ticker format) only for the download request; stored data and metadata remain in dot form. Records are deduplicated and processed in batches of up to 100 tickers so the rate limits of `yfinance`/Yahoo Finance stay manageable. No API key is required because validation relies on the public `yfinance.download` interface.

## Development

`uv run pytest -v`

## Limitations

- The ETF roster is the curated `ETF_SPECS` dictionary in `src/etf_universe/registry.py`; please align any README updates with that source.
- Each fetch delivers only the latest snapshot published by the provider; historical snapshots must be stored externally.
- Invesco-backed ETFs require the Chromium browser because their holdings page is scraped via Playwright rather than a static XLSX bundle.
