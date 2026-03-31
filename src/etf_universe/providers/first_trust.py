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
    if not table_rows:
        raise ValueError("Holdings table contains no rows")

    header_cells = table_rows[0].find_all(["td", "th"], recursive=False)
    headers = [cell.get_text(" ", strip=True) for cell in header_cells]
    header_to_index = {header: index for index, header in enumerate(headers)}

    def resolve_header(name_options: list[str], purpose: str) -> int:
        for name in name_options:
            if name in header_to_index:
                return header_to_index[name]
        raise ValueError(
            f"Missing required header for {purpose}; expected one of {name_options}, found {headers}"
        )

    name_index = resolve_header(["Security Name"], "security name")
    symbol_index = resolve_header(["Identifier", "Ticker", "Symbol"], "ticker")
    classification_index = resolve_header(["Classification", "Security Type"], "classification")
    weight_index = resolve_header(["Weighting"], "weighting")

    def get_cell_text(cells: list[Any], index: int) -> str:
        if index < len(cells):
            return cells[index].get_text(" ", strip=True)
        return ""

    records: list[SourceHoldingRow] = []
    for row in table_rows[1:]:
        cells = row.find_all("td", recursive=False)
        if not cells:
            continue
        name = get_cell_text(cells, name_index)
        symbol = get_cell_text(cells, symbol_index)
        classification = get_cell_text(cells, classification_index)
        weight = get_cell_text(cells, weight_index)
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
