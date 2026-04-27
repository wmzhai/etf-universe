from datetime import date, datetime, timezone

import pytest

from etf_universe.contracts import EtfProfile, EtfSpec, FetchResult, SourceHoldingRow
from etf_universe.normalization import (
    collect_candidate_symbols,
    normalize_for_storage,
    parse_date_from_text,
    parse_float,
    normalize_symbol,
    parse_date,
)


def test_normalize_symbol_trims_uppercases_and_rewrites_share_class() -> None:
    assert normalize_symbol(" brk/b ") == "BRK.B"


def test_normalize_symbol_rejects_lowercase_placeholder_values() -> None:
    assert normalize_symbol(" none ") is None
    assert normalize_symbol("null") is None
    assert normalize_symbol("n/a") is None
    assert normalize_symbol("na") == "NA"


def test_parse_date_supports_multiple_formats() -> None:
    assert parse_date("Mar 28, 2026") == date(2026, 3, 28)
    assert parse_date("2026-03-28") == date(2026, 3, 28)


def test_parse_date_from_text_extracts_matching_date() -> None:
    assert (
        parse_date_from_text(r"As of ([A-Za-z]+ \d{1,2}, \d{4})", "Holdings As of Mar 28, 2026")
        == date(2026, 3, 28)
    )


def test_parse_float_supports_percent_and_parentheses() -> None:
    assert parse_float("6.1%") == 6.1
    assert parse_float("(1.25)") == -1.25


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


def test_collect_candidate_symbols_rejects_placeholder_and_cash_rows() -> None:
    fetch_result = FetchResult(
        as_of_date=date(2026, 3, 28),
        source_url="https://example.com/source",
        source_format="csv",
        rows=[
            SourceHoldingRow("AAPL", "Apple", 6.1),
            SourceHoldingRow(" none ", "Placeholder", 0.1),
            SourceHoldingRow("USD", "Cash Position", 0.2, asset_class="Cash", security_type="Currency"),
        ],
    )

    assert collect_candidate_symbols(fetch_result) == ["AAPL"]


def test_collect_candidate_symbols_rejects_unclassified_currency_placeholders() -> None:
    fetch_result = FetchResult(
        as_of_date=date(2026, 3, 28),
        source_url="https://example.com/source",
        source_format="csv",
        rows=[
            SourceHoldingRow("AAPL", "Apple", 6.1),
            SourceHoldingRow("USD", "US Dollar", 0.2),
        ],
    )

    assert collect_candidate_symbols(fetch_result) == ["AAPL"]


@pytest.mark.parametrize(
    ("currency_symbol", "currency_name"),
    [
        ("EUR", "Euro"),
        ("JPY", "Japanese Yen"),
        ("GBP", "British Pound"),
        ("CHF", "Swiss Franc"),
    ],
)
def test_collect_candidate_symbols_rejects_iso_currency_code_name_pairs(
    currency_symbol: str,
    currency_name: str,
) -> None:
    fetch_result = FetchResult(
        as_of_date=date(2026, 3, 28),
        source_url="https://example.com/source",
        source_format="csv",
        rows=[
            SourceHoldingRow("AAPL", "Apple", 6.1),
            SourceHoldingRow(currency_symbol, currency_name, 0.2),
        ],
    )

    assert collect_candidate_symbols(fetch_result) == ["AAPL"]


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


def test_normalize_for_storage_carries_profile_into_meta() -> None:
    spec = EtfSpec(
        symbol="SOXX",
        group="Layer 2",
        issuer="iShares",
        provider="ishares",
        source_url="https://example.com/soxx.csv",
    )
    profile = EtfProfile(
        fundName="iShares Semiconductor ETF",
        exchange="NASDAQ",
        assetClass="Equity",
        inceptionDate="2001-07-10",
        expenseRatio=0.34,
        assetsUnderManagement=30418500216.0,
        sharesOutstanding=65900000.0,
        secYield30Day=0.27,
        distributionFrequency="Quarterly",
        profileAsOfDate="2026-04-24",
        profileSourceUrl="https://example.com/soxx",
    )
    fetch_result = FetchResult(
        as_of_date=date(2026, 4, 24),
        source_url="https://example.com/soxx.csv",
        source_format="csv",
        rows=[SourceHoldingRow("AMD", "Advanced Micro Devices", 7.88)],
        profile=profile,
    )

    rows, meta = normalize_for_storage(
        spec=spec,
        fetched_at=datetime(2026, 4, 27, 12, 0, tzinfo=timezone.utc),
        fetch_result=fetch_result,
        valid_symbols={"AMD"},
    )

    assert [row.symbol for row in rows] == ["AMD"]
    assert meta.profile == profile


def test_normalize_for_storage_rejects_cash_like_rows_before_symbol_validation() -> None:
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
            SourceHoldingRow("USD", "Cash Position", 0.2, asset_class="Cash", security_type="Currency"),
        ],
    )

    rows, meta = normalize_for_storage(
        spec=spec,
        fetched_at=datetime(2026, 3, 31, 12, 0, tzinfo=timezone.utc),
        fetch_result=fetch_result,
        valid_symbols={"AAPL", "USD"},
    )

    assert [row.symbol for row in rows] == ["AAPL"]
    assert meta.normalizedRowCount == 1
    assert meta.droppedRowCount == 1


def test_normalize_for_storage_rejects_unclassified_currency_placeholders() -> None:
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
            SourceHoldingRow("USD", "US Dollar", 0.2),
        ],
    )

    rows, meta = normalize_for_storage(
        spec=spec,
        fetched_at=datetime(2026, 3, 31, 12, 0, tzinfo=timezone.utc),
        fetch_result=fetch_result,
        valid_symbols={"AAPL", "USD"},
    )

    assert [row.symbol for row in rows] == ["AAPL"]
    assert meta.normalizedRowCount == 1
    assert meta.droppedRowCount == 1


@pytest.mark.parametrize(
    ("currency_symbol", "currency_name"),
    [
        ("EUR", "Euro"),
        ("JPY", "Japanese Yen"),
        ("GBP", "British Pound"),
        ("CHF", "Swiss Franc"),
    ],
)
def test_normalize_for_storage_rejects_iso_currency_code_name_pairs(
    currency_symbol: str,
    currency_name: str,
) -> None:
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
            SourceHoldingRow(currency_symbol, currency_name, 0.2),
        ],
    )

    rows, meta = normalize_for_storage(
        spec=spec,
        fetched_at=datetime(2026, 3, 31, 12, 0, tzinfo=timezone.utc),
        fetch_result=fetch_result,
        valid_symbols={"AAPL", currency_symbol},
    )

    assert [row.symbol for row in rows] == ["AAPL"]
    assert meta.normalizedRowCount == 1
    assert meta.droppedRowCount == 1
