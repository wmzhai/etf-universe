from __future__ import annotations

import time
from typing import Any

import requests

from etf_universe.contracts import SourceHoldingRow
from etf_universe.normalization import clean_text, parse_float
from etf_universe.runtime_logging import elapsed_ms, log_event


USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
)
HTTP_TIMEOUT = 60


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    return session


def request_with_logging(
    session: Any,
    method: str,
    url: str,
    *,
    timeout: int | float = HTTP_TIMEOUT,
    **kwargs: Any,
) -> Any:
    method_name = method.upper()
    log_event("http.request", method=method_name, url=url, timeout=timeout)
    started_at = time.perf_counter()
    try:
        response = session.request(method_name, url, timeout=timeout, **kwargs)
    except Exception as exc:
        log_event(
            "http.error",
            method=method_name,
            url=url,
            error_type=type(exc).__name__,
            error=str(exc),
            elapsed_ms=elapsed_ms(started_at),
        )
        raise

    response_url = getattr(response, "url", url)
    response_content = getattr(response, "content", b"") or b""
    log_event(
        "http.response",
        method=method_name,
        url=response_url,
        status=getattr(response, "status_code", "unknown"),
        bytes=len(response_content),
        elapsed_ms=elapsed_ms(started_at),
    )
    return response


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
