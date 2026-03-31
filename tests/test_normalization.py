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
