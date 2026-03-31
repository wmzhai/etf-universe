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
