# ETF Universe Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `etf-universe` as a standalone Python package managed with `uv` that exposes a stable CLI for listing supported ETFs and fetching holdings snapshots for a curated ETF subset into parquet plus JSON metadata outputs.

**Architecture:** Split the package into a thin CLI, a registry layer, normalization and storage helpers, batched `yfinance`-based symbol validation, and provider-specific fetch modules. Keep the CLI as the only public interface for v1 so agents and humans all use the same entry point.

**Tech Stack:** Python 3.12, `uv`, `requests`, `beautifulsoup4`, `openpyxl`, `pyarrow`, `playwright`, `pytest`

---

## File Map

- Create: `pyproject.toml`
- Create: `README.md`
- Create: `AGENTS.md`
- Create: `src/etf_universe/__init__.py`
- Create: `src/etf_universe/cli.py`
- Create: `src/etf_universe/contracts.py`
- Create: `src/etf_universe/registry.py`
- Create: `src/etf_universe/normalization.py`
- Create: `src/etf_universe/storage.py`
- Create: `src/etf_universe/validation.py`
- Create: `src/etf_universe/providers/__init__.py`
- Create: `src/etf_universe/providers/base.py`
- Create: `src/etf_universe/providers/ssga.py`
- Create: `src/etf_universe/providers/ishares.py`
- Create: `src/etf_universe/providers/vaneck.py`
- Create: `src/etf_universe/providers/first_trust.py`
- Create: `src/etf_universe/providers/invesco.py`
- Create: `tests/test_registry.py`
- Create: `tests/test_cli_list_supported.py`
- Create: `tests/test_normalization.py`
- Create: `tests/test_storage.py`
- Create: `tests/test_validation.py`
- Create: `tests/test_cli_fetch.py`
- Create: `tests/test_readme.py`
- Create: `tests/providers/test_ssga.py`
- Create: `tests/providers/test_ishares.py`
- Create: `tests/providers/test_vaneck.py`
- Create: `tests/providers/test_first_trust.py`
- Create: `tests/providers/test_invesco.py`

### Task 1: Bootstrap the package and `list-supported` CLI

**Files:**
- Create: `pyproject.toml`
- Create: `src/etf_universe/__init__.py`
- Create: `src/etf_universe/contracts.py`
- Create: `src/etf_universe/registry.py`
- Create: `src/etf_universe/cli.py`
- Test: `tests/test_registry.py`
- Test: `tests/test_cli_list_supported.py`

- [ ] **Step 1: Write the failing tests for registry parsing and the `list-supported` command**

```python
# tests/test_registry.py
from etf_universe.registry import ETF_SPECS, list_supported_symbols, parse_symbols_arg


def test_list_supported_symbols_is_sorted() -> None:
    assert list_supported_symbols() == sorted(ETF_SPECS)


def test_parse_symbols_arg_normalizes_order_and_dedupes() -> None:
    assert parse_symbols_arg(" qqq , spy, qqq ") == ["QQQ", "SPY"]
```

```python
# tests/test_cli_list_supported.py
from etf_universe.cli import main


def test_holdings_list_supported_prints_one_symbol_per_line(capsys) -> None:
    exit_code = main(["holdings", "list-supported"])

    assert exit_code == 0
    lines = capsys.readouterr().out.strip().splitlines()
    assert lines == sorted(lines)
    assert "SPY" in lines
    assert "QQQ" in lines
```

- [ ] **Step 2: Run the tests to confirm the package does not exist yet**

Run:

```bash
uv run pytest tests/test_registry.py tests/test_cli_list_supported.py -v
```

Expected:

- FAIL with import errors such as `ModuleNotFoundError: No module named 'etf_universe'`

- [ ] **Step 3: Create the package scaffold, registry, and minimal CLI**

```toml
# pyproject.toml
[build-system]
requires = ["hatchling>=1.27.0"]
build-backend = "hatchling.build"

[project]
name = "etf-universe"
version = "0.1.0"
description = "Curated ETF holdings fetcher and exporter."
readme = "README.md"
requires-python = ">=3.12"
dependencies = [
  "beautifulsoup4>=4.12.3",
  "openpyxl>=3.1.5",
  "playwright>=1.52.0",
  "pyarrow>=20.0.0",
  "requests>=2.32.3",
]

[dependency-groups]
dev = ["pytest>=8.3.5"]

[project.scripts]
etf-universe = "etf_universe.cli:main"

[tool.pytest.ini_options]
pythonpath = ["src"]
addopts = "-ra"
testpaths = ["tests"]
```

```python
# src/etf_universe/__init__.py
__all__ = ["__version__"]

__version__ = "0.1.0"
```

```python
# src/etf_universe/contracts.py
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class EtfSpec:
    symbol: str
    group: str
    issuer: str
    provider: str
    source_url: str
```

```python
# src/etf_universe/registry.py
from __future__ import annotations

from etf_universe.contracts import EtfSpec


ETF_SPECS: dict[str, EtfSpec] = {
    "SPY": EtfSpec("SPY", "Layer 0", "SSGA", "ssga", "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-spy.xlsx"),
    "QQQ": EtfSpec("QQQ", "Layer 0", "Invesco", "invesco", "https://www.invesco.com/qqq-etf/en/about.html"),
    "DIA": EtfSpec("DIA", "Layer 0", "SSGA", "ssga", "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-dia.xlsx"),
    "IWM": EtfSpec("IWM", "Layer 0", "iShares", "ishares", "https://www.ishares.com/us/products/239710/ishares-russell-2000-etf/1467271812596.ajax?fileType=csv"),
    "XLK": EtfSpec("XLK", "Layer 1", "SSGA", "ssga", "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xlk.xlsx"),
    "XLF": EtfSpec("XLF", "Layer 1", "SSGA", "ssga", "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xlf.xlsx"),
    "XLE": EtfSpec("XLE", "Layer 1", "SSGA", "ssga", "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xle.xlsx"),
    "XLV": EtfSpec("XLV", "Layer 1", "SSGA", "ssga", "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xlv.xlsx"),
    "XLY": EtfSpec("XLY", "Layer 1", "SSGA", "ssga", "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xly.xlsx"),
    "XLP": EtfSpec("XLP", "Layer 1", "SSGA", "ssga", "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xlp.xlsx"),
    "XLI": EtfSpec("XLI", "Layer 1", "SSGA", "ssga", "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xli.xlsx"),
    "XLB": EtfSpec("XLB", "Layer 1", "SSGA", "ssga", "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xlb.xlsx"),
    "XLU": EtfSpec("XLU", "Layer 1", "SSGA", "ssga", "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xlu.xlsx"),
    "XLRE": EtfSpec("XLRE", "Layer 1", "SSGA", "ssga", "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xlre.xlsx"),
    "XLC": EtfSpec("XLC", "Layer 1", "SSGA", "ssga", "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xlc.xlsx"),
    "SMH": EtfSpec("SMH", "Layer 2", "VanEck", "vaneck", "https://www.vaneck.com/us/en/investments/semiconductor-etf-smh/holdings/"),
    "IGV": EtfSpec("IGV", "Layer 2", "iShares", "ishares", "https://www.ishares.com/us/products/239771/ishares-north-american-techsoftware-etf/1467271812596.ajax?fileType=csv"),
    "KRE": EtfSpec("KRE", "Layer 2", "SSGA", "ssga", "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-kre.xlsx"),
    "KBE": EtfSpec("KBE", "Layer 2", "SSGA", "ssga", "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-kbe.xlsx"),
    "XOP": EtfSpec("XOP", "Layer 2", "SSGA", "ssga", "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xop.xlsx"),
    "OIH": EtfSpec("OIH", "Layer 2", "VanEck", "vaneck", "https://www.vaneck.com/us/en/investments/oil-services-etf-oih/holdings/"),
    "XBI": EtfSpec("XBI", "Layer 2", "SSGA", "ssga", "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xbi.xlsx"),
    "IHI": EtfSpec("IHI", "Layer 2", "iShares", "ishares", "https://www.ishares.com/us/products/239516/ishares-us-medical-devices-etf/1467271812596.ajax?fileType=csv"),
    "XRT": EtfSpec("XRT", "Layer 2", "SSGA", "ssga", "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xrt.xlsx"),
    "ITA": EtfSpec("ITA", "Layer 2", "iShares", "ishares", "https://www.ishares.com/us/products/239502/ishares-us-aerospace-defense-etf/1467271812596.ajax?fileType=csv"),
    "IYT": EtfSpec("IYT", "Layer 2", "iShares", "ishares", "https://www.ishares.com/us/products/239501/ishares-transportation-average-etf/1467271812596.ajax?fileType=csv"),
    "GDX": EtfSpec("GDX", "Layer 2", "VanEck", "vaneck", "https://www.vaneck.com/us/en/investments/gold-miners-etf-gdx/holdings/"),
    "FDN": EtfSpec("FDN", "Layer 2", "First Trust", "first_trust", "https://www.ftportfolios.com/Retail/Etf/EtfHoldings.aspx?Ticker=FDN"),
    "RSP": EtfSpec("RSP", "Breadth", "Invesco", "invesco", "https://www.invesco.com/us/en/financial-products/etfs/invesco-sp-500-equal-weight-etf.html"),
    "QQEW": EtfSpec("QQEW", "Breadth", "First Trust", "first_trust", "https://www.ftportfolios.com/Retail/Etf/EtfHoldings.aspx?Ticker=QQEW"),
}


def list_supported_symbols() -> list[str]:
    return sorted(ETF_SPECS)


def parse_symbols_arg(raw: str) -> list[str]:
    seen: set[str] = set()
    symbols: list[str] = []

    for item in raw.split(","):
        symbol = item.strip().upper()
        if not symbol:
            continue
        if symbol not in ETF_SPECS:
            raise SystemExit(f"Unknown ETF symbols: {symbol}")
        if symbol in seen:
            continue
        seen.add(symbol)
        symbols.append(symbol)

    if not symbols:
        raise SystemExit("No ETF symbols supplied")

    return symbols


def get_specs(symbols: list[str]) -> list[EtfSpec]:
    return [ETF_SPECS[symbol] for symbol in symbols]
```

```python
# src/etf_universe/cli.py
from __future__ import annotations

import argparse

from etf_universe.registry import list_supported_symbols


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="etf-universe")
    top_level = parser.add_subparsers(dest="topic")

    holdings = top_level.add_parser("holdings")
    holdings_subcommands = holdings.add_subparsers(dest="holdings_command")

    list_supported = holdings_subcommands.add_parser("list-supported")
    list_supported.set_defaults(func=run_holdings_list_supported)

    return parser


def run_holdings_list_supported(args: argparse.Namespace) -> int:
    del args
    for symbol in list_supported_symbols():
        print(symbol)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if not hasattr(args, "func"):
        parser.print_help()
        return 1

    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Run the tests to verify the minimal package passes**

Run:

```bash
uv run pytest tests/test_registry.py tests/test_cli_list_supported.py -v
```

Expected:

- PASS for both test files

- [ ] **Step 5: Commit the bootstrap**

```bash
git add pyproject.toml src tests
git commit -m "feat: bootstrap etf-universe package"
```

### Task 2: Add shared contracts and normalization rules

**Files:**
- Modify: `src/etf_universe/contracts.py`
- Create: `src/etf_universe/normalization.py`
- Test: `tests/test_normalization.py`

- [ ] **Step 1: Write the failing normalization tests**

```python
# tests/test_normalization.py
from datetime import date, datetime, timezone

from etf_universe.contracts import EtfSpec, FetchResult, SourceHoldingRow
from etf_universe.normalization import (
    collect_candidate_symbols,
    normalize_for_storage,
    normalize_symbol,
    parse_date,
)


def test_normalize_symbol_trims_uppercases_and_rewrites_share_class() -> None:
    assert normalize_symbol(" brk/b ") == "BRK.B"


def test_parse_date_supports_multiple_formats() -> None:
    assert parse_date("Mar 28, 2026") == date(2026, 3, 28)
    assert parse_date("2026-03-28") == date(2026, 3, 28)


def test_collect_candidate_symbols_filters_invalid_rows() -> None:
    fetch_result = FetchResult(
        as_of_date=date(2026, 3, 28),
        source_url="https://example.com/source",
        source_format="csv",
        rows=[
            SourceHoldingRow("AAPL", "Apple", 6.1),
            SourceHoldingRow(" brk/b ", "Berkshire", 1.9),
            SourceHoldingRow("CASH AND OTHER", "Cash", 0.2),
            SourceHoldingRow(None, "Missing", 0.1),
        ],
    )

    assert collect_candidate_symbols(fetch_result) == ["AAPL", "BRK.B"]


def test_normalize_for_storage_builds_rows_and_meta() -> None:
    spec = EtfSpec(
        symbol="SPY",
        group="Layer 0",
        issuer="SSGA",
        provider="ssga",
        source_url="https://example.com/spy.xlsx",
    )
    fetch_result = FetchResult(
        as_of_date=date(2026, 3, 28),
        source_url="https://example.com/spy.xlsx",
        source_format="xlsx",
        rows=[
            SourceHoldingRow("AAPL", "Apple", 6.1),
            SourceHoldingRow(" BRK/B ", "Berkshire", 1.9),
            SourceHoldingRow("CASH AND OTHER", "Cash", 0.2),
        ],
    )

    rows, meta = normalize_for_storage(
        spec=spec,
        fetched_at=datetime(2026, 3, 31, 12, 0, tzinfo=timezone.utc),
        fetch_result=fetch_result,
        valid_symbols={"AAPL", "BRK.B"},
    )

    assert [row.symbol for row in rows] == ["AAPL", "BRK.B"]
    assert meta.etfSymbol == "SPY"
    assert meta.normalizedRowCount == 2
    assert meta.droppedRowCount == 1
```

- [ ] **Step 2: Run the normalization tests and confirm the new imports fail**

Run:

```bash
uv run pytest tests/test_normalization.py -v
```

Expected:

- FAIL with import errors for `SourceHoldingRow`, `FetchResult`, or `normalize_for_storage`

- [ ] **Step 3: Expand contracts and implement normalization helpers**

```python
# src/etf_universe/contracts.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class EtfSpec:
    symbol: str
    group: str
    issuer: str
    provider: str
    source_url: str


@dataclass(frozen=True)
class SourceHoldingRow:
    constituent_symbol: str | None
    constituent_name: str | None
    weight: float | None
    asset_class: str | None = None
    security_type: str | None = None


@dataclass(frozen=True)
class FetchResult:
    as_of_date: date
    source_url: str
    source_format: str
    rows: list[SourceHoldingRow]


@dataclass(frozen=True)
class NormalizedHoldingRow:
    symbol: str
    name: str | None
    weight: float | None


@dataclass(frozen=True)
class HoldingsMeta:
    schemaVersion: str
    etfSymbol: str
    issuer: str
    provider: str
    asOfDate: str
    fetchedAt: str
    sourceUrl: str
    sourceFormat: str
    rowCount: int
    normalizedRowCount: int
    droppedRowCount: int
```

```python
# src/etf_universe/normalization.py
from __future__ import annotations

import html
import re
from datetime import date, datetime, timezone
from typing import Any

from etf_universe.contracts import FetchResult, HoldingsMeta, NormalizedHoldingRow, EtfSpec


META_SCHEMA_VERSION = "2026-03-31.etf-universe-meta.v1"
ALLOWED_EQUITY_SYMBOL_PATTERN = re.compile(r"^[A-Z][A-Z0-9]*(?:\.[A-Z0-9]+)*$")


def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = html.unescape(str(value)).strip()
    if not text or text in {"-", "--", "—", "N/A", "nan", "None"}:
        return None
    return text


def parse_float(value: Any) -> float | None:
    text = clean_text(value)
    if text is None:
        return None
    text = text.replace(",", "").replace("$", "").replace("%", "")
    if text.startswith("(") and text.endswith(")"):
        text = f"-{text[1:-1]}"
    try:
        return float(text)
    except ValueError:
        return None


def parse_date(value: Any) -> date:
    text = clean_text(value)
    if text is None:
        raise ValueError("missing date")
    text = re.sub(r"\bas of\b", "", text, flags=re.IGNORECASE).strip()
    text = re.sub(r"\s+", " ", text)
    if "T" in text:
        text = text.split("T", 1)[0]
    for fmt in ("%d-%b-%Y", "%b %d, %Y", "%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"unsupported date format: {text}")


def parse_date_from_text(pattern: str, text: str) -> date:
    match = re.search(pattern, text, flags=re.IGNORECASE)
    if not match:
        raise ValueError(f"date pattern not found: {pattern}")
    return parse_date(match.group(1))


def normalize_symbol(value: str | None) -> str | None:
    text = clean_text(value)
    if text is None:
        return None
    return text.upper().replace("/", ".")


def is_supported_equity_symbol(symbol: str) -> bool:
    return bool(ALLOWED_EQUITY_SYMBOL_PATTERN.fullmatch(symbol))


def collect_candidate_symbols(fetch_result: FetchResult) -> list[str]:
    symbols: list[str] = []
    for row in fetch_result.rows:
        normalized_symbol = normalize_symbol(row.constituent_symbol)
        if normalized_symbol is None:
            continue
        if not is_supported_equity_symbol(normalized_symbol):
            continue
        symbols.append(normalized_symbol)
    return symbols


def normalize_for_storage(
    spec: EtfSpec,
    fetched_at: datetime,
    fetch_result: FetchResult,
    valid_symbols: set[str] | None = None,
) -> tuple[list[NormalizedHoldingRow], HoldingsMeta]:
    normalized_rows: list[NormalizedHoldingRow] = []
    dropped_row_count = 0

    for row in fetch_result.rows:
        normalized_symbol = normalize_symbol(row.constituent_symbol)
        if normalized_symbol is None:
            dropped_row_count += 1
            continue
        if not is_supported_equity_symbol(normalized_symbol):
            dropped_row_count += 1
            continue
        if valid_symbols is not None and normalized_symbol not in valid_symbols:
            dropped_row_count += 1
            continue
        normalized_rows.append(
            NormalizedHoldingRow(
                symbol=normalized_symbol,
                name=row.constituent_name,
                weight=row.weight,
            )
        )

    if not normalized_rows:
        raise ValueError(f"{spec.symbol}: no usable holdings rows after normalization")

    meta = HoldingsMeta(
        schemaVersion=META_SCHEMA_VERSION,
        etfSymbol=spec.symbol,
        issuer=spec.issuer,
        provider=spec.provider,
        asOfDate=fetch_result.as_of_date.isoformat(),
        fetchedAt=fetched_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        sourceUrl=fetch_result.source_url,
        sourceFormat=fetch_result.source_format,
        rowCount=len(fetch_result.rows),
        normalizedRowCount=len(normalized_rows),
        droppedRowCount=dropped_row_count,
    )
    return normalized_rows, meta
```

- [ ] **Step 4: Run the normalization tests and verify they pass**

Run:

```bash
uv run pytest tests/test_normalization.py -v
```

Expected:

- PASS for all normalization tests

- [ ] **Step 5: Commit the normalization layer**

```bash
git add src/etf_universe/contracts.py src/etf_universe/normalization.py tests/test_normalization.py
git commit -m "feat: add holdings normalization contract"
```

### Task 3: Add parquet and metadata writers

**Files:**
- Create: `src/etf_universe/storage.py`
- Test: `tests/test_storage.py`

- [ ] **Step 1: Write the failing storage tests**

```python
# tests/test_storage.py
import json

import pyarrow.parquet as pq

from etf_universe.contracts import HoldingsMeta, NormalizedHoldingRow
from etf_universe.storage import write_meta, write_parquet


def test_write_parquet_persists_expected_rows(tmp_path) -> None:
    output_path = tmp_path / "SPY.parquet"
    rows = [
        NormalizedHoldingRow(symbol="AAPL", name="Apple", weight=6.1),
        NormalizedHoldingRow(symbol="BRK.B", name="Berkshire", weight=1.9),
    ]

    write_parquet(rows, output_path)

    table = pq.read_table(output_path)
    assert table.column("symbol").to_pylist() == ["AAPL", "BRK.B"]
    assert table.column("weight").to_pylist() == [6.1, 1.9]


def test_write_meta_persists_json_sidecar(tmp_path) -> None:
    output_path = tmp_path / "SPY.meta.json"
    meta = HoldingsMeta(
        schemaVersion="2026-03-31.etf-universe-meta.v1",
        etfSymbol="SPY",
        issuer="SSGA",
        provider="ssga",
        asOfDate="2026-03-28",
        fetchedAt="2026-03-31T12:00:00Z",
        sourceUrl="https://example.com/spy.xlsx",
        sourceFormat="xlsx",
        rowCount=503,
        normalizedRowCount=503,
        droppedRowCount=0,
    )

    write_meta(meta, output_path)

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["etfSymbol"] == "SPY"
    assert payload["sourceFormat"] == "xlsx"
```

- [ ] **Step 2: Run the storage tests to confirm the module is missing**

Run:

```bash
uv run pytest tests/test_storage.py -v
```

Expected:

- FAIL with `ModuleNotFoundError` for `etf_universe.storage`

- [ ] **Step 3: Implement parquet and metadata writing**

```python
# src/etf_universe/storage.py
from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from etf_universe.contracts import HoldingsMeta, NormalizedHoldingRow


PARQUET_SCHEMA = pa.schema(
    [
        ("symbol", pa.string()),
        ("name", pa.string()),
        ("weight", pa.float64()),
    ]
)


def write_parquet(rows: list[NormalizedHoldingRow], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist([asdict(row) for row in rows], schema=PARQUET_SCHEMA)
    pq.write_table(table, output_path, compression="zstd")


def write_meta(meta: HoldingsMeta, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(f"{json.dumps(asdict(meta), indent=2)}\n", encoding="utf-8")
```

- [ ] **Step 4: Run the storage tests to verify the files are written correctly**

Run:

```bash
uv run pytest tests/test_storage.py -v
```

Expected:

- PASS for both storage tests

- [ ] **Step 5: Commit the storage layer**

```bash
git add src/etf_universe/storage.py tests/test_storage.py
git commit -m "feat: add parquet and metadata storage"
```

### Task 4: Add yfinance-backed symbol validation

**Files:**
- Modify: `pyproject.toml`
- Create: `src/etf_universe/validation.py`
- Test: `tests/test_validation.py`

- [ ] **Step 1: Write the failing validation tests**

```python
# tests/test_validation.py
import pandas as pd

from etf_universe.validation import YFinanceSymbolValidator, normalize_symbol_for_yahoo


def make_batch_frame(payload_by_symbol: dict[str, list[dict[str, float]]]) -> pd.DataFrame:
    return pd.concat(
        {symbol: pd.DataFrame(rows) for symbol, rows in payload_by_symbol.items()},
        axis=1,
    )


def test_normalize_symbol_for_yahoo_rewrites_share_class() -> None:
    assert normalize_symbol_for_yahoo("BRK.B") == "BRK-B"
    assert normalize_symbol_for_yahoo("AAPL") == "AAPL"


def test_validator_keeps_symbols_with_real_ohlcv_data(monkeypatch) -> None:
    download_frame = make_batch_frame(
        {
            "AAPL": [
                {"Open": 1.0, "High": 1.1, "Low": 0.9, "Close": 1.05, "Volume": 100.0}
            ],
            "BRK-B": [
                {"Open": 2.0, "High": 2.1, "Low": 1.9, "Close": 2.05, "Volume": 200.0}
            ],
            "INVALIDZZZZ": [
                {
                    "Open": float("nan"),
                    "High": float("nan"),
                    "Low": float("nan"),
                    "Close": float("nan"),
                    "Volume": float("nan"),
                }
            ],
        }
    )

    monkeypatch.setattr(
        "etf_universe.validation.yf.download",
        lambda *args, **kwargs: download_frame,
    )

    validator = YFinanceSymbolValidator(batch_size=50)
    valid_symbols = validator.validate_symbols(["AAPL", "BRK.B", "INVALIDZZZZ"])

    assert valid_symbols == {"AAPL", "BRK.B"}


def test_validator_handles_single_symbol_download_shape(monkeypatch) -> None:
    download_frame = pd.DataFrame(
        [
            {"Open": 2.0, "High": 2.1, "Low": 1.9, "Close": 2.05, "Volume": 200.0}
        ]
    )

    monkeypatch.setattr(
        "etf_universe.validation.yf.download",
        lambda *args, **kwargs: download_frame,
    )

    validator = YFinanceSymbolValidator(batch_size=50)
    valid_symbols = validator.validate_symbols(["BRK.B"])

    assert valid_symbols == {"BRK.B"}


def test_validator_splits_large_symbol_sets_into_batches(monkeypatch) -> None:
    calls: list[list[str]] = []

    def fake_download(symbols, **kwargs):  # noqa: ANN001
        if isinstance(symbols, str):
            requested = [symbols]
        else:
            requested = list(symbols)
        calls.append(requested)

        if len(requested) == 1:
            return pd.DataFrame(
                [{"Open": 1.0, "High": 1.1, "Low": 0.9, "Close": 1.05, "Volume": 100.0}]
            )

        return make_batch_frame(
            {
                symbol: [
                    {"Open": 1.0, "High": 1.1, "Low": 0.9, "Close": 1.05, "Volume": 100.0}
                ]
                for symbol in requested
            }
        )

    monkeypatch.setattr("etf_universe.validation.yf.download", fake_download)

    validator = YFinanceSymbolValidator(batch_size=2)
    valid_symbols = validator.validate_symbols(["AAPL", "MSFT", "NVDA"])

    assert valid_symbols == {"AAPL", "MSFT", "NVDA"}
    assert calls == [["AAPL", "MSFT"], ["NVDA"]]
```

- [ ] **Step 2: Run the validation tests to confirm the validator module is missing**

Run:

```bash
uv run pytest tests/test_validation.py -v
```

Expected:

- FAIL with `ModuleNotFoundError` for `etf_universe.validation`

- [ ] **Step 3: Implement the validator and helpers**

```toml
# pyproject.toml
dependencies = [
  "beautifulsoup4>=4.12.3",
  "openpyxl>=3.1.5",
  "pandas>=3.0.2",
  "playwright>=1.52.0",
  "pyarrow>=20.0.0",
  "requests>=2.32.3",
  "yfinance>=1.2.0",
]
```

```python
# src/etf_universe/validation.py
from __future__ import annotations

import yfinance as yf

YFINANCE_VALIDATION_PERIOD = "5d"
YFINANCE_VALIDATION_INTERVAL = "1d"
YFINANCE_SYMBOL_BATCH_SIZE = 100
YFINANCE_REQUIRED_COLUMNS = ("Open", "High", "Low", "Close", "Volume")

from etf_universe.normalization import normalize_symbol


def dedupe_symbols(symbols: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for symbol in symbols:
        if symbol in seen:
            continue
        seen.add(symbol)
        deduped.append(symbol)
    return deduped


def chunk_symbols(symbols: list[str], batch_size: int) -> list[list[str]]:
    return [
        symbols[index : index + batch_size]
        for index in range(0, len(symbols), batch_size)
    ]


def normalize_symbol_for_yahoo(symbol: str) -> str:
    return symbol.replace(".", "-")


def has_usable_ohlcv_rows(data) -> bool:  # noqa: ANN001
    if data.empty:
        return False

    try:
        ohlcv = data[list(YFINANCE_REQUIRED_COLUMNS)]
    except KeyError:
        return False

    if ohlcv.dropna(how="all").empty:
        return False

    if ohlcv["Close"].dropna().empty and ohlcv["Volume"].dropna().empty:
        return False

    return True


class YFinanceSymbolValidator:
    def __init__(self, batch_size: int = YFINANCE_SYMBOL_BATCH_SIZE) -> None:
        self._batch_size = batch_size
        self._cache: dict[str, bool] = {}

    @property
    def enabled(self) -> bool:
        return True

    def validate_symbols(self, symbols: list[str]) -> set[str]:
        deduped_symbols = dedupe_symbols(symbols)
        valid_symbols: set[str] = set()
        for batch in chunk_symbols(deduped_symbols, self._batch_size):
            valid_symbols.update(self._validate_batch(batch))
        return valid_symbols

    def _validate_batch(self, symbols: list[str]) -> set[str]:
        yahoo_symbols = [normalize_symbol_for_yahoo(symbol) for symbol in symbols]
        download_frame = yf.download(
            yahoo_symbols if len(yahoo_symbols) > 1 else yahoo_symbols[0],
            period=YFINANCE_VALIDATION_PERIOD,
            interval=YFINANCE_VALIDATION_INTERVAL,
            group_by="ticker",
            auto_adjust=False,
            progress=False,
            threads=True,
        )

        valid_symbols: set[str] = set()
        for original_symbol, yahoo_symbol in zip(symbols, yahoo_symbols):
            try:
                data = download_frame if len(yahoo_symbols) == 1 else download_frame[yahoo_symbol]
            except Exception:
                self._cache[original_symbol] = False
                continue

            normalized_symbol = normalize_symbol(original_symbol)
            if normalized_symbol is None:
                self._cache[original_symbol] = False
                continue

            if has_usable_ohlcv_rows(data):
                self._cache[original_symbol] = True
                valid_symbols.add(normalized_symbol)
            else:
                self._cache[original_symbol] = False

        return valid_symbols
```

- [ ] **Step 4: Run the validator tests to verify the retry logic**

Run:

```bash
uv run pytest tests/test_validation.py -v
```

Expected:

- PASS for all validator tests

- [ ] **Step 5: Commit the validator**

```bash
git add pyproject.toml src/etf_universe/validation.py tests/test_validation.py
git commit -m "feat: add yfinance symbol validation"
```

### Task 5: Implement shared provider helpers plus SSGA and iShares fetchers

**Files:**
- Create: `src/etf_universe/providers/base.py`
- Create: `src/etf_universe/providers/ssga.py`
- Create: `src/etf_universe/providers/ishares.py`
- Test: `tests/providers/test_ssga.py`
- Test: `tests/providers/test_ishares.py`

- [ ] **Step 1: Write the failing parser tests for SSGA and iShares**

```python
# tests/providers/test_ssga.py
from io import BytesIO

from openpyxl import Workbook

from etf_universe.providers.ssga import parse_ssga_workbook


def test_parse_ssga_workbook_extracts_holdings_rows() -> None:
    workbook = Workbook()
    sheet = workbook.active
    sheet.append([])
    sheet.append([])
    sheet.append(["As of Mar 28, 2026", None])
    sheet.append([])
    sheet.append(["Ticker", "Name", "Weight"])
    sheet.append(["AAPL", "Apple Inc.", "6.1"])
    sheet.append(["BRK/B", "Berkshire Hathaway Inc.", "1.9"])

    buffer = BytesIO()
    workbook.save(buffer)

    result = parse_ssga_workbook(buffer.getvalue(), "https://example.com/spy.xlsx")

    assert result.source_format == "xlsx"
    assert result.as_of_date.isoformat() == "2026-03-28"
    assert [row.constituent_symbol for row in result.rows] == ["AAPL", "BRK/B"]
```

```python
# tests/providers/test_ishares.py
from etf_universe.providers.ishares import parse_ishares_csv


def test_parse_ishares_csv_extracts_as_of_date_and_rows() -> None:
    csv_text = """Fund Holdings as of,Mar 28, 2026
Ticker,Name,Sector,Asset Class,Weight (%),Security Type
AAPL,Apple Inc.,Technology,Equity,6.10,Common Stock
MSFT,Microsoft Corp.,Technology,Equity,5.90,Common Stock
"""

    result = parse_ishares_csv(csv_text, "https://example.com/iwm.csv")

    assert result.source_format == "csv"
    assert result.as_of_date.isoformat() == "2026-03-28"
    assert [row.constituent_symbol for row in result.rows] == ["AAPL", "MSFT"]
```

- [ ] **Step 2: Run the provider tests and confirm the modules are missing**

Run:

```bash
uv run pytest tests/providers/test_ssga.py tests/providers/test_ishares.py -v
```

Expected:

- FAIL with `ModuleNotFoundError` for the provider modules

- [ ] **Step 3: Implement the shared provider helpers plus SSGA and iShares fetchers**

```python
# src/etf_universe/providers/base.py
from __future__ import annotations

from typing import Any

import requests

from etf_universe.contracts import SourceHoldingRow
from etf_universe.normalization import clean_text, parse_float


USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
)
HTTP_TIMEOUT = 60


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    return session


def get_by_header(row: tuple[Any, ...], index: dict[str, int], header: str) -> Any:
    position = index.get(header)
    if position is None:
        return None
    return row[position]


def build_source_row(
    *,
    constituent_symbol: Any,
    constituent_name: Any,
    weight: Any,
    asset_class: Any = None,
    security_type: Any = None,
) -> SourceHoldingRow:
    return SourceHoldingRow(
        constituent_symbol=clean_text(constituent_symbol),
        constituent_name=clean_text(constituent_name),
        weight=parse_float(weight),
        asset_class=clean_text(asset_class),
        security_type=clean_text(security_type),
    )
```

```python
# src/etf_universe/providers/ssga.py
from __future__ import annotations

import io

from openpyxl import load_workbook

from etf_universe.contracts import EtfSpec, FetchResult, SourceHoldingRow
from etf_universe.normalization import clean_text, parse_date_from_text
from etf_universe.providers.base import HTTP_TIMEOUT, build_source_row, get_by_header


def parse_ssga_workbook(content: bytes, source_url: str) -> FetchResult:
    workbook = load_workbook(io.BytesIO(content), data_only=True, read_only=True)
    sheet = workbook[workbook.sheetnames[0]]
    rows = list(sheet.iter_rows(values_only=True))
    as_of_date = parse_date_from_text(
        r"As of\s+(.+)",
        " ".join(str(cell or "") for cell in rows[2][:2]),
    )

    headers = [clean_text(value) for value in rows[4]]
    index = {header: i for i, header in enumerate(headers) if header}
    records: list[SourceHoldingRow] = []

    for row in rows[5:]:
        if not any(row):
            continue

        name = row[index["Name"]]
        symbol = row[index["Ticker"]]
        if clean_text(name) is None and clean_text(symbol) is None:
            continue

        records.append(
            build_source_row(
                constituent_symbol=symbol,
                constituent_name=name,
                weight=get_by_header(row, index, "Weight"),
                asset_class="Equity",
                security_type="Common Stock",
            )
        )

    return FetchResult(
        as_of_date=as_of_date,
        source_url=source_url,
        source_format="xlsx",
        rows=records,
    )


def fetch_ssga(spec: EtfSpec, session) -> FetchResult:  # noqa: ANN001
    response = session.get(spec.source_url, timeout=HTTP_TIMEOUT)
    response.raise_for_status()
    return parse_ssga_workbook(response.content, spec.source_url)
```

```python
# src/etf_universe/providers/ishares.py
from __future__ import annotations

import csv
import io
from datetime import date

from etf_universe.contracts import EtfSpec, FetchResult, SourceHoldingRow
from etf_universe.normalization import clean_text, parse_date, parse_float
from etf_universe.providers.base import HTTP_TIMEOUT, build_source_row


def parse_ishares_csv(text: str, source_url: str) -> FetchResult:
    rows = list(csv.reader(io.StringIO(text.lstrip("\ufeff"))))
    header_idx = next(
        i
        for i, row in enumerate(rows)
        if len(row) >= 4 and row[0] == "Ticker" and row[1] == "Name" and row[2] == "Sector"
    )

    as_of_date: date | None = None
    for row in rows[:header_idx]:
        for idx, cell in enumerate(row):
            if "Fund Holdings as of" in cell:
                candidate = row[idx + 1] if idx + 1 < len(row) else cell.split("Fund Holdings as of", 1)[-1]
                as_of_date = parse_date(candidate)
                break
        if as_of_date:
            break

    if as_of_date is None:
        raise ValueError("Unable to find as-of date in iShares CSV")

    headers = rows[header_idx]
    records: list[SourceHoldingRow] = []
    for raw_row in rows[header_idx + 1 :]:
        if len(raw_row) < len(headers):
            continue
        row = dict(zip(headers, raw_row))
        if clean_text(row.get("Ticker")) is None and clean_text(row.get("Name")) is None:
            continue
        if parse_float(row.get("Weight (%)")) is None:
            continue
        records.append(
            build_source_row(
                constituent_symbol=row.get("Ticker"),
                constituent_name=row.get("Name"),
                weight=row.get("Weight (%)"),
                asset_class=row.get("Asset Class"),
                security_type=row.get("Security Type"),
            )
        )

    return FetchResult(
        as_of_date=as_of_date,
        source_url=source_url,
        source_format="csv",
        rows=records,
    )


def fetch_ishares(spec: EtfSpec, session) -> FetchResult:  # noqa: ANN001
    response = session.get(spec.source_url, timeout=HTTP_TIMEOUT)
    response.raise_for_status()
    return parse_ishares_csv(response.text, spec.source_url)
```

- [ ] **Step 4: Run the SSGA and iShares tests**

Run:

```bash
uv run pytest tests/providers/test_ssga.py tests/providers/test_ishares.py -v
```

Expected:

- PASS for both provider test files

- [ ] **Step 5: Commit the first provider set**

```bash
git add src/etf_universe/providers tests/providers
git commit -m "feat: add ssga and ishares providers"
```

### Task 6: Implement VanEck, First Trust, and Invesco fetchers

**Files:**
- Create: `src/etf_universe/providers/vaneck.py`
- Create: `src/etf_universe/providers/first_trust.py`
- Create: `src/etf_universe/providers/invesco.py`
- Test: `tests/providers/test_vaneck.py`
- Test: `tests/providers/test_first_trust.py`
- Test: `tests/providers/test_invesco.py`

- [ ] **Step 1: Write the failing tests for VanEck, First Trust, and Invesco parsing**

```python
# tests/providers/test_vaneck.py
from etf_universe.providers.vaneck import extract_dataset_url, parse_vaneck_payload


def test_extract_dataset_url_finds_symbol_specific_json_endpoint() -> None:
    html_text = '<script>{"contentUrl":"https://api.example.test/holdings?ticker=SMH"}</script>'
    assert extract_dataset_url(html_text, "SMH") == "https://api.example.test/holdings?ticker=SMH"


def test_parse_vaneck_payload_builds_fetch_result() -> None:
    payload = {
        "HoldingsList": [
            {
                "AsOfDate": "2026-03-28",
                "Holdings": [
                    {"Label": "NVDA", "HoldingName": "NVIDIA Corp.", "Weight": "20.3", "AssetClass": "Equity", "SecurityType": "Common Stock"},
                    {"Label": "TSM", "HoldingName": "Taiwan Semiconductor", "Weight": "11.2", "AssetClass": "Equity", "SecurityType": "Common Stock"},
                ],
            }
        ]
    }

    result = parse_vaneck_payload(payload, "https://api.example.test/holdings?ticker=SMH")

    assert result.as_of_date.isoformat() == "2026-03-28"
    assert [row.constituent_symbol for row in result.rows] == ["NVDA", "TSM"]
```

```python
# tests/providers/test_first_trust.py
from etf_universe.providers.first_trust import parse_first_trust_html


def test_parse_first_trust_html_targets_fund_silver_grid() -> None:
    html_text = """
    <html>
      <body>
        <span id="ctl00_MainContent_lblHoldingsTitle">Holdings as of Mar 28, 2026</span>
        <table class="fundSilverGrid">
          <tr>
            <th>Security Name</th><th>Ticker</th><th>Sector</th><th>Classification</th><th>X</th><th>Y</th><th>Weighting</th>
          </tr>
          <tr>
            <td>Amazon.com Inc.</td><td>AMZN</td><td>Consumer</td><td>Common Stock</td><td></td><td></td><td>5.1%</td>
          </tr>
        </table>
      </body>
    </html>
    """

    result = parse_first_trust_html(html_text, "https://example.com/fdn")

    assert result.as_of_date.isoformat() == "2026-03-28"
    assert result.rows[0].constituent_symbol == "AMZN"
```

```python
# tests/providers/test_invesco.py
from etf_universe.providers.invesco import build_rsp_api_url, parse_invesco_payload


def test_build_rsp_api_url_uses_isin_to_derive_cusip() -> None:
    assert build_rsp_api_url("US46137V3574").endswith("/46137V357/holdings/fund?idType=cusip&productType=ETF")


def test_parse_invesco_payload_extracts_holdings_rows() -> None:
    payload = {
        "effectiveDate": "2026-03-28",
        "holdings": [
            {
                "ticker": "AAPL",
                "issuerName": "Apple Inc.",
                "percentageOfTotalNetAssets": "6.1",
                "assetClassName": "Equity",
                "securityTypeName": "Common Stock",
            }
        ],
    }

    result = parse_invesco_payload(payload, "https://api.example.test/qqq")

    assert result.as_of_date.isoformat() == "2026-03-28"
    assert result.rows[0].constituent_symbol == "AAPL"
```

- [ ] **Step 2: Run the provider tests and confirm the new modules are missing**

Run:

```bash
uv run pytest tests/providers/test_vaneck.py tests/providers/test_first_trust.py tests/providers/test_invesco.py -v
```

Expected:

- FAIL with `ModuleNotFoundError` for the new provider modules

- [ ] **Step 3: Implement VanEck, First Trust, and Invesco modules**

```python
# src/etf_universe/providers/vaneck.py
from __future__ import annotations

import html
import re
from typing import Any

from etf_universe.contracts import EtfSpec, FetchResult, SourceHoldingRow
from etf_universe.normalization import clean_text, parse_date
from etf_universe.providers.base import HTTP_TIMEOUT, build_source_row


def extract_dataset_url(html_text: str, symbol: str) -> str:
    match = re.search(r'"contentUrl":"([^"]+ticker=' + re.escape(symbol) + r')"', html_text)
    if not match:
        raise ValueError(f"Unable to find VanEck dataset URL for {symbol}")
    return html.unescape(match.group(1))


def parse_vaneck_payload(payload: dict[str, Any], source_url: str) -> FetchResult:
    holdings_block = payload["HoldingsList"][0]
    as_of_date = parse_date(holdings_block["AsOfDate"])
    records: list[SourceHoldingRow] = []

    for row in holdings_block["Holdings"]:
        symbol = row.get("Label")
        name = row.get("HoldingName")
        if clean_text(symbol) is None and clean_text(name) is None:
            continue
        records.append(
            build_source_row(
                constituent_symbol=symbol,
                constituent_name=name or symbol,
                weight=row.get("Weight"),
                asset_class=row.get("AssetClass"),
                security_type=row.get("SecurityType") or row.get("Classification"),
            )
        )

    return FetchResult(
        as_of_date=as_of_date,
        source_url=source_url,
        source_format="json",
        rows=records,
    )


def fetch_vaneck(spec: EtfSpec, session) -> FetchResult:  # noqa: ANN001
    page_response = session.get(spec.source_url, timeout=HTTP_TIMEOUT)
    page_response.raise_for_status()
    dataset_url = extract_dataset_url(page_response.text, spec.symbol)

    dataset_response = session.get(dataset_url, timeout=HTTP_TIMEOUT)
    dataset_response.raise_for_status()
    return parse_vaneck_payload(dataset_response.json(), dataset_url)
```

```python
# src/etf_universe/providers/first_trust.py
from __future__ import annotations

from typing import Any

from bs4 import BeautifulSoup

from etf_universe.contracts import EtfSpec, FetchResult, SourceHoldingRow
from etf_universe.normalization import clean_text, parse_date_from_text
from etf_universe.providers.base import HTTP_TIMEOUT, build_source_row


def get_direct_table_rows(table: Any) -> list[Any]:
    body = table.find("tbody", recursive=False)
    row_parent = body if body is not None else table
    return row_parent.find_all("tr", recursive=False)


def parse_first_trust_html(html_text: str, source_url: str) -> FetchResult:
    soup = BeautifulSoup(html_text, "html.parser")
    title = soup.find(id=lambda value: bool(value) and value.endswith("lblHoldingsTitle"))
    if title is None:
        raise ValueError("Unable to find holdings title")
    as_of_date = parse_date_from_text(r"as of\s+(.+)", title.get_text(" ", strip=True))

    target_table = soup.select_one("table.fundSilverGrid")
    if target_table is None:
        raise ValueError("Unable to find holdings table")

    table_rows = get_direct_table_rows(target_table)
    header_cells = table_rows[0].find_all(["td", "th"], recursive=False)
    headers = [cell.get_text(" ", strip=True) for cell in header_cells]
    if "Security Name" not in headers or "Weighting" not in headers:
        raise ValueError(f"Unexpected holdings table headers: {headers}")

    records: list[SourceHoldingRow] = []
    for tr in table_rows[1:]:
        cells = tr.find_all("td", recursive=False)
        if len(cells) < 7:
            continue
        name, symbol, _, classification, _, _, weight = [
            cell.get_text(" ", strip=True) for cell in cells[:7]
        ]
        if clean_text(symbol) is None and clean_text(name) is None:
            continue
        records.append(
            build_source_row(
                constituent_symbol=symbol,
                constituent_name=name,
                weight=weight,
                asset_class="Equity",
                security_type=classification,
            )
        )

    return FetchResult(
        as_of_date=as_of_date,
        source_url=source_url,
        source_format="html",
        rows=records,
    )


def fetch_first_trust(spec: EtfSpec, session) -> FetchResult:  # noqa: ANN001
    response = session.get(spec.source_url, timeout=HTTP_TIMEOUT)
    response.raise_for_status()
    return parse_first_trust_html(response.text, spec.source_url)
```

```python
# src/etf_universe/providers/invesco.py
from __future__ import annotations

import json
from typing import Any

from playwright.sync_api import Browser, Page, sync_playwright

from etf_universe.contracts import EtfSpec, FetchResult, SourceHoldingRow
from etf_universe.normalization import clean_text, parse_date
from etf_universe.providers.base import build_source_row


def browser_fetch_json(page: Page, api_url: str) -> dict[str, Any]:
    result = page.evaluate(
        """async (url) => {
            const response = await fetch(url.replace(/&amp;/g, '&'));
            return { status: response.status, text: await response.text() };
        }""",
        api_url,
    )
    if result["status"] != 200:
        raise ValueError(f"Browser fetch failed: {result['status']} {api_url}")
    return json.loads(result["text"])


def build_rsp_api_url(isin: str) -> str:
    cleaned = isin.strip()
    if len(cleaned) < 11:
        raise ValueError("Unable to derive CUSIP from RSP ISIN")
    cusip = cleaned[2:11]
    return (
        "https://dng-api.invesco.com/cache/v1/accounts/en_US/shareclasses/"
        f"{cusip}/holdings/fund?idType=cusip&productType=ETF"
    )


def parse_invesco_payload(payload: dict[str, Any], source_url: str) -> FetchResult:
    as_of_date = parse_date(payload["effectiveDate"])
    records: list[SourceHoldingRow] = []

    for row in payload["holdings"]:
        symbol = row.get("ticker")
        name = row.get("issuerName")
        if clean_text(symbol) is None and clean_text(name) is None:
            continue
        records.append(
            build_source_row(
                constituent_symbol=symbol,
                constituent_name=name,
                weight=row.get("percentageOfTotalNetAssets"),
                asset_class=row.get("assetClassName") or row.get("assetClass"),
                security_type=row.get("securityTypeName") or row.get("securityTypeCode"),
            )
        )

    return FetchResult(
        as_of_date=as_of_date,
        source_url=source_url,
        source_format="json-browser",
        rows=records,
    )


def fetch_invesco(spec: EtfSpec, page: Page) -> FetchResult:
    page.goto(spec.source_url, wait_until="domcontentloaded", timeout=120000)

    if spec.symbol == "QQQ":
        locator = page.locator("[data-holding-api]").first
        locator.wait_for(state="attached", timeout=60000)
        api_url = locator.get_attribute("data-holding-api")
        if not api_url:
            raise ValueError("Unable to find QQQ data-holding-api")
    elif spec.symbol == "RSP":
        locator = page.locator('meta[name="isin"]').first
        locator.wait_for(state="attached", timeout=60000)
        isin = locator.get_attribute("content")
        if not isin:
            raise ValueError("Unable to find RSP ISIN")
        api_url = build_rsp_api_url(isin)
    else:
        raise ValueError(f"Unsupported Invesco symbol: {spec.symbol}")

    source_url = api_url.replace("&amp;", "&")
    payload = browser_fetch_json(page, api_url)
    return parse_invesco_payload(payload, source_url)


def launch_browser() -> tuple[Any, Browser, Page]:
    playwright = sync_playwright().start()
    try:
        try:
            browser = playwright.chromium.launch(channel="chrome", headless=True)
        except Exception:
            browser = playwright.chromium.launch(headless=True)
    except Exception:
        playwright.stop()
        raise
    return playwright, browser, browser.new_page()


def close_browser(playwright: Any, browser: Browser) -> None:
    browser.close()
    playwright.stop()
```

- [ ] **Step 4: Run the second provider batch tests**

Run:

```bash
uv run pytest tests/providers/test_vaneck.py tests/providers/test_first_trust.py tests/providers/test_invesco.py -v
```

Expected:

- PASS for all three provider test files

- [ ] **Step 5: Commit the remaining provider modules**

```bash
git add src/etf_universe/providers tests/providers
git commit -m "feat: add vaneck first-trust and invesco providers"
```

### Task 7: Implement the `holdings fetch` orchestration command

**Files:**
- Create: `src/etf_universe/providers/__init__.py`
- Modify: `src/etf_universe/cli.py`
- Test: `tests/test_cli_fetch.py`

- [ ] **Step 1: Write the failing CLI fetch test**

```python
# tests/test_cli_fetch.py
from datetime import date

from etf_universe.cli import main
from etf_universe.contracts import FetchResult, SourceHoldingRow


class FakeValidator:
    enabled = True

    def validate_symbols(self, symbols: list[str]) -> set[str]:
        return set(symbols)


def test_holdings_fetch_prints_summary_and_writes_expected_outputs(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    writes: list[tuple[str, str, str]] = []

    def fake_fetch_with_provider(spec, session, page=None):  # noqa: ANN001
        del session, page
        return FetchResult(
            as_of_date=date(2026, 3, 28),
            source_url=spec.source_url,
            source_format="csv",
            rows=[SourceHoldingRow("AAPL", "Apple Inc.", 6.1)],
        )

    monkeypatch.setattr("etf_universe.cli.make_session", lambda: object())
    monkeypatch.setattr("etf_universe.cli.build_symbol_validator", lambda session: FakeValidator())
    monkeypatch.setattr("etf_universe.cli.fetch_with_provider", fake_fetch_with_provider)
    monkeypatch.setattr(
        "etf_universe.cli.write_parquet",
        lambda rows, path: writes.append(("parquet", path.name, rows[0].symbol)),
    )
    monkeypatch.setattr(
        "etf_universe.cli.write_meta",
        lambda meta, path: writes.append(("meta", path.name, meta.etfSymbol)),
    )

    exit_code = main(
        [
            "holdings",
            "fetch",
            "--symbols",
            "SPY",
            "--output-dir",
            str(tmp_path),
        ]
    )

    assert exit_code == 0
    assert ("parquet", "SPY.parquet", "AAPL") in writes
    assert ("meta", "SPY.meta.json", "SPY") in writes
    assert "SPY: kept=1 dropped=0 as_of=2026-03-28 provider=SSGA" in capsys.readouterr().out
```

- [ ] **Step 2: Run the fetch CLI test and confirm the command is not implemented**

Run:

```bash
uv run pytest tests/test_cli_fetch.py -v
```

Expected:

- FAIL because the `fetch` subcommand or imported helpers do not exist yet

- [ ] **Step 3: Add provider dispatch and the full fetch command**

```python
# src/etf_universe/providers/__init__.py
from __future__ import annotations

from etf_universe.contracts import EtfSpec, FetchResult
from etf_universe.providers.base import make_session
from etf_universe.providers.first_trust import fetch_first_trust
from etf_universe.providers.invesco import close_browser, fetch_invesco, launch_browser
from etf_universe.providers.ishares import fetch_ishares
from etf_universe.providers.ssga import fetch_ssga
from etf_universe.providers.vaneck import fetch_vaneck


PROVIDER_FETCHERS = {
    "ssga": fetch_ssga,
    "ishares": fetch_ishares,
    "vaneck": fetch_vaneck,
    "first_trust": fetch_first_trust,
    "invesco": fetch_invesco,
}


def fetch_with_provider(spec: EtfSpec, session, page=None) -> FetchResult:  # noqa: ANN001
    fetcher = PROVIDER_FETCHERS[spec.provider]
    if spec.provider == "invesco":
        if page is None:
            raise ValueError("Invesco fetch requires an initialized browser page")
        return fetcher(spec, page)
    return fetcher(spec, session)
```

```python
# src/etf_universe/cli.py
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

from etf_universe.normalization import collect_candidate_symbols, normalize_for_storage
from etf_universe.providers import close_browser, fetch_with_provider, launch_browser, make_session
from etf_universe.registry import get_specs, list_supported_symbols, parse_symbols_arg
from etf_universe.storage import write_meta, write_parquet
from etf_universe.validation import YFinanceSymbolValidator


DEFAULT_OUTPUT_DIR = Path("data/etf-holdings")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="etf-universe")
    top_level = parser.add_subparsers(dest="topic")

    holdings = top_level.add_parser("holdings")
    holdings_subcommands = holdings.add_subparsers(dest="holdings_command")

    list_supported = holdings_subcommands.add_parser("list-supported")
    list_supported.set_defaults(func=run_holdings_list_supported)

    fetch = holdings_subcommands.add_parser("fetch")
    fetch.add_argument("--symbols", required=True, help="Comma-separated ETF symbol list")
    fetch.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory for parquet and metadata outputs",
    )
    fetch.set_defaults(func=run_holdings_fetch)

    return parser


def build_symbol_validator(session) -> YFinanceSymbolValidator:  # noqa: ANN001
    del session
    return YFinanceSymbolValidator()


def run_holdings_list_supported(args: argparse.Namespace) -> int:
    del args
    for symbol in list_supported_symbols():
        print(symbol)
    return 0


def run_holdings_fetch(args: argparse.Namespace) -> int:
    output_dir = Path(args.output_dir)
    symbols = parse_symbols_arg(args.symbols)
    specs = get_specs(symbols)
    session = make_session()
    validator = build_symbol_validator(session)
    fetched_at = datetime.now(timezone.utc)

    playwright = None
    browser = None
    page = None
    fetched_results = {}
    candidate_symbols: list[str] = []

    try:
        if any(spec.provider == "invesco" for spec in specs):
            playwright, browser, page = launch_browser()

        for spec in specs:
            fetch_result = fetch_with_provider(spec, session=session, page=page)
            fetched_results[spec.symbol] = fetch_result
            candidate_symbols.extend(collect_candidate_symbols(fetch_result))

        valid_symbols = validator.validate_symbols(candidate_symbols)

        for spec in specs:
            fetch_result = fetched_results[spec.symbol]
            normalized_rows, meta = normalize_for_storage(
                spec=spec,
                fetched_at=fetched_at,
                fetch_result=fetch_result,
                valid_symbols=valid_symbols,
            )
            write_parquet(normalized_rows, output_dir / f"{spec.symbol}.parquet")
            write_meta(meta, output_dir / f"{spec.symbol}.meta.json")
            print(
                f"{spec.symbol}: kept={meta.normalizedRowCount} dropped={meta.droppedRowCount} "
                f"as_of={meta.asOfDate} provider={spec.issuer}",
                flush=True,
            )
    finally:
        if playwright is not None and browser is not None:
            close_browser(playwright, browser)

    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if not hasattr(args, "func"):
        parser.print_help()
        return 1

    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Run the fetch CLI test**

Run:

```bash
uv run pytest tests/test_cli_fetch.py tests/test_cli_list_supported.py -v
```

Expected:

- PASS for fetch and list-supported CLI tests

- [ ] **Step 5: Commit the public CLI**

```bash
git add src/etf_universe/providers/__init__.py src/etf_universe/cli.py tests/test_cli_fetch.py
git commit -m "feat: add holdings fetch command"
```

### Task 8: Write the public docs and lock README to the registry

**Files:**
- Create: `README.md`
- Create: `AGENTS.md`
- Test: `tests/test_readme.py`

- [ ] **Step 1: Write the failing README consistency test**

```python
# tests/test_readme.py
import re
from pathlib import Path

from etf_universe.registry import list_supported_symbols


def test_readme_supported_etfs_matches_registry() -> None:
    readme_path = Path("README.md")
    content = readme_path.read_text(encoding="utf-8")
    match = re.search(
        r"<!-- supported-etfs:start -->(.*?)<!-- supported-etfs:end -->",
        content,
        flags=re.DOTALL,
    )
    assert match is not None

    supported_symbols = re.findall(r"`([A-Z.]+)`", match.group(1))
    assert supported_symbols == list_supported_symbols()
```

- [ ] **Step 2: Run the README test and confirm the docs do not exist yet**

Run:

```bash
uv run pytest tests/test_readme.py -v
```

Expected:

- FAIL because `README.md` and `AGENTS.md` do not exist yet

- [ ] **Step 3: Write the README and repository instructions**

````markdown
# ETF Universe

`etf-universe` is a standalone Python package that fetches current holdings for a curated ETF subset and writes one parquet file plus one metadata sidecar file per ETF.

## Installation

```bash
uv sync
```

If you need the Playwright browser runtime for Invesco fetches:

```bash
uv run playwright install chromium
```

## CLI

List the supported ETFs:

```bash
uv run etf-universe holdings list-supported
```

Fetch holdings into a local output directory:

```bash
uv run etf-universe holdings fetch --symbols SPY,QQQ --output-dir ./data/etf-holdings
```

## Supported ETFs

<!-- supported-etfs:start -->
- `DIA`
- `FDN`
- `GDX`
- `IGV`
- `IHI`
- `ITA`
- `IWM`
- `IYT`
- `KBE`
- `KRE`
- `OIH`
- `QQEW`
- `QQQ`
- `RSP`
- `SMH`
- `SPY`
- `XBI`
- `XLB`
- `XLC`
- `XLE`
- `XLF`
- `XLI`
- `XLK`
- `XLP`
- `XLRE`
- `XLU`
- `XLV`
- `XLY`
- `XOP`
- `XRT`
<!-- supported-etfs:end -->

## Output Format

Each requested ETF produces:

- `{output_dir}/{TICKER}.parquet`
- `{output_dir}/{TICKER}.meta.json`

Parquet schema:

- `symbol`
- `name`
- `weight`

Metadata fields:

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

## Symbol Validation

`etf-universe` uses batched `yfinance` downloads for symbol validation in version 1.

- No API key is required
- Dot-form share classes such as `BRK.B` are translated to Yahoo-compatible dash form such as `BRK-B` only for validation
- Stored holdings output remains in normalized dot form
- Validation runs in batches to reduce rate limiting risk

## Development

Run the test suite:

```bash
uv run pytest -v
```

## Limitations

- Version 1 supports a curated ETF subset only
- Version 1 keeps only the latest snapshot for each ETF in the target output directory
- Invesco fetches require Playwright because the holdings API is discovered in a browser context
````

```markdown
# Repository Instructions

- Keep repository docs, specs, plans, and commit messages in English.
- Use `uv` for install, run, and test workflows.
- Keep the CLI examples aligned with the public `etf-universe` command.
- Treat `README.md` as user-facing documentation and keep the supported ETF list aligned with `src/etf_universe/registry.py`.
```

- [ ] **Step 4: Run the README consistency test and the full suite**

Run:

```bash
uv run pytest -v
```

Expected:

- PASS for the README consistency test
- PASS for the full package test suite

- [ ] **Step 5: Commit the public docs**

```bash
git add README.md AGENTS.md tests/test_readme.py
git commit -m "docs: add public package documentation"
```

## Self-Review

- Spec coverage:
  - Python package with `uv`: covered in Task 1 and Task 8
  - Curated ETF registry: covered in Task 1
  - Shared output contract: covered in Tasks 2 and 3
  - yfinance validation: covered in Task 4
  - Provider-specific fetchers: covered in Tasks 5 and 6
  - Public CLI for `list-supported` and `fetch`: covered in Tasks 1 and 7
  - English-only docs and commit messages: covered in Task 8

- Placeholder scan:
  - No placeholder markers remain inside the executable plan steps

- Type consistency:
  - `EtfSpec`, `FetchResult`, `SourceHoldingRow`, `NormalizedHoldingRow`, and `HoldingsMeta` are named consistently across all tasks
  - `parse_symbols_arg`, `list_supported_symbols`, `normalize_for_storage`, `write_parquet`, `write_meta`, and `YFinanceSymbolValidator` retain consistent names through the full plan

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-03-31-etf-universe.md`. Two execution options:

**1. Subagent-Driven (recommended)** - I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** - Execute tasks in this session using executing-plans, batch execution with checkpoints

Which approach?
