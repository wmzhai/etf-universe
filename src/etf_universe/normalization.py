from __future__ import annotations

import html
import re
from dataclasses import asdict
from datetime import date, datetime, timezone
from typing import Any

from etf_universe.contracts import FetchResult, HoldingsMeta, NormalizedHoldingRow, EtfSpec, SourceHoldingRow


ALLOWED_EQUITY_SYMBOL_PATTERN = re.compile(r"^[A-Z][A-Z0-9]*(?:\.[A-Z0-9]+)*$")
PLACEHOLDER_TEXT_VALUES = frozenset({"-", "--", "—", "n/a", "nan", "none", "null"})
CASH_LIKE_FIELD_TERMS = ("cash", "cash equivalent", "currency", "money market")
NON_HOLDING_NAME_TERMS = ("cash and other", "cash position", "other assets", "other liabilities")
ISO_CURRENCY_PLACEHOLDERS = {
    "USD": frozenset({"us dollar"}),
    "EUR": frozenset({"euro"}),
    "JPY": frozenset({"japanese yen"}),
    "GBP": frozenset({"british pound"}),
    "CHF": frozenset({"swiss franc"}),
}


def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = html.unescape(str(value)).strip()
    if not text or text.casefold() in PLACEHOLDER_TEXT_VALUES:
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


def is_unclassified_currency_placeholder(row: SourceHoldingRow) -> bool:
    normalized_symbol = normalize_symbol(row.constituent_symbol)
    name = clean_text(row.constituent_name)
    if normalized_symbol is None or name is None:
        return False

    currency_names = ISO_CURRENCY_PLACEHOLDERS.get(normalized_symbol)
    if currency_names is None:
        return False

    return name.casefold() in currency_names


def is_locally_eligible_holding_row(row: SourceHoldingRow) -> bool:
    for value in (row.asset_class, row.security_type):
        text = clean_text(value)
        if text is None:
            continue
        lowered = text.casefold()
        if any(term in lowered for term in CASH_LIKE_FIELD_TERMS):
            return False

    name = clean_text(row.constituent_name)
    if name is not None:
        lowered_name = name.casefold()
        if any(term in lowered_name for term in NON_HOLDING_NAME_TERMS):
            return False
    if is_unclassified_currency_placeholder(row):
        return False

    return True


def collect_candidate_symbols(fetch_result: FetchResult) -> list[str]:
    symbols: list[str] = []
    for row in fetch_result.rows:
        if not is_locally_eligible_holding_row(row):
            continue
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
        if not is_locally_eligible_holding_row(row):
            dropped_row_count += 1
            continue
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
        **asdict(fetch_result.profile),
    )
    return normalized_rows, meta
