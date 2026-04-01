from __future__ import annotations

import html
import re
from typing import Any
from urllib.parse import parse_qs, urlparse

from etf_universe.contracts import EtfSpec, FetchResult, SourceHoldingRow
from etf_universe.normalization import clean_text, parse_date
from etf_universe.providers.base import HTTP_TIMEOUT, build_source_row, request_with_logging


def extract_dataset_url(html_text: str, symbol: str) -> str:
    pattern = r'"contentUrl"\s*:\s*"([^"]*?ticker=' + re.escape(symbol) + r'[^"]*?)"'
    for match in re.finditer(pattern, html_text):
        candidate = html.unescape(match.group(1))
        parsed = urlparse(candidate)
        ticker_values = parse_qs(parsed.query).get("ticker")
        if ticker_values and any(value == symbol for value in ticker_values):
            return candidate
    raise ValueError(f"Unable to find VanEck dataset URL for {symbol}")


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
                constituent_name=name,
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
    page_response = request_with_logging(session, "GET", spec.source_url, timeout=HTTP_TIMEOUT)
    page_response.raise_for_status()
    dataset_url = extract_dataset_url(page_response.text, spec.symbol)

    dataset_response = request_with_logging(session, "GET", dataset_url, timeout=HTTP_TIMEOUT)
    dataset_response.raise_for_status()
    return parse_vaneck_payload(dataset_response.json(), dataset_url)
