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
        row_text = ",".join(row)
        if "Fund Holdings as of" in row_text:
            candidate = row_text.split("Fund Holdings as of", 1)[-1].strip(" ,")
            as_of_date = parse_date(candidate)
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
        asset_class = clean_text(row.get("Asset Class"))
        if asset_class is not None and asset_class.casefold() != "equity":
            continue
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
