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
