from __future__ import annotations

import csv
import io
from datetime import date

from etf_universe.contracts import EtfSpec, FetchResult, SourceHoldingRow
from etf_universe.normalization import clean_text, parse_date
from etf_universe.providers.base import HTTP_TIMEOUT, build_source_row, request_with_logging


REQUIRED_HEADERS = {"date", "company", "ticker", "weight (%)"}


def parse_ark_csv(text: str, source_url: str) -> FetchResult:
    reader = csv.DictReader(io.StringIO(text.lstrip("\ufeff")))
    headers = set(reader.fieldnames or [])
    missing_headers = REQUIRED_HEADERS - headers
    if missing_headers:
        raise ValueError(f"Missing required ARK CSV headers: {sorted(missing_headers)}")

    as_of_date: date | None = None
    records: list[SourceHoldingRow] = []
    for row in reader:
        if clean_text(row.get("ticker")) is None and clean_text(row.get("company")) is None:
            continue

        row_date = parse_date(row.get("date"))
        if as_of_date is None:
            as_of_date = row_date
        elif row_date != as_of_date:
            raise ValueError("ARK CSV contains mixed as-of dates")

        records.append(
            build_source_row(
                constituent_symbol=row.get("ticker"),
                constituent_name=row.get("company"),
                weight=row.get("weight (%)"),
                asset_class="Equity",
            )
        )

    if as_of_date is None:
        raise ValueError("Unable to find as-of date in ARK CSV")

    return FetchResult(
        as_of_date=as_of_date,
        source_url=source_url,
        source_format="csv",
        rows=records,
    )


def fetch_ark(spec: EtfSpec, session) -> FetchResult:  # noqa: ANN001
    response = request_with_logging(session, "GET", spec.source_url, timeout=HTTP_TIMEOUT)
    response.raise_for_status()
    return parse_ark_csv(response.text, spec.source_url)
