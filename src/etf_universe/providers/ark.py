from __future__ import annotations

import csv
import io
import re
from datetime import date
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from etf_universe.contracts import EtfProfile, EtfSpec, FetchResult, SourceHoldingRow
from etf_universe.normalization import clean_text, parse_date
from etf_universe.profile import merge_profiles
from etf_universe.providers.base import HTTP_TIMEOUT, build_source_row, request_with_logging


REQUIRED_HEADERS = {"date", "company", "ticker", "weight (%)"}
ARK_EXPENSE_PROFILE_URL = (
    "https://helpcenter.ark-funds.com/what-is-the-fee-structure-expense-ratio-of-ark-etfs"
)
ARK_FUND_NAMES = {
    "ARKF": "ARK Fintech Innovation ETF",
    "ARKG": "ARK Genomic Revolution ETF",
    "ARKK": "ARK Innovation ETF",
    "ARKQ": "ARK Autonomous Tech. & Robotics ETF",
    "ARKW": "ARK Next Generation Internet ETF",
    "ARKX": "ARK Space Exploration & Innovation ETF",
}


def parse_ark_expense_profile_html(html_text: str, symbol: str, source_url: str) -> EtfProfile:
    soup = BeautifulSoup(html_text, "html.parser")
    text = re.sub(r"\s+", " ", soup.get_text(" ", strip=True))

    default_ratio = None
    default_match = re.search(
        r"actively managed ETFs\s+is\s+([0-9]+(?:\.[0-9]+)?)\s*%",
        text,
        flags=re.IGNORECASE,
    )
    if default_match:
        default_ratio = float(default_match.group(1))

    ratio = default_ratio
    exception_match = re.search(
        rf"\b{re.escape(symbol)}\b\s+which\s+is\s+([0-9]+(?:\.[0-9]+)?)\s*%",
        text,
        flags=re.IGNORECASE,
    )
    if exception_match:
        ratio = float(exception_match.group(1))

    return EtfProfile(
        expenseRatio=ratio,
        profileSourceUrl=source_url if ratio is not None else None,
    )


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

    symbol = None
    path = urlparse(source_url).path
    for candidate in ARK_FUND_NAMES:
        if f"_{candidate}_HOLDINGS.csv" in path:
            symbol = candidate
            break
    if symbol is None and records:
        symbol = clean_text(next(csv.DictReader(io.StringIO(text.lstrip("\ufeff")))).get("fund"))

    return FetchResult(
        as_of_date=as_of_date,
        source_url=source_url,
        source_format="csv",
        rows=records,
        profile=EtfProfile(
            fundName=ARK_FUND_NAMES.get(symbol or ""),
            assetClass="Equity",
            fundType="ETF",
            profileAsOfDate=as_of_date.isoformat(),
            profileSourceUrl=source_url,
        ),
    )


def fetch_ark(spec: EtfSpec, session) -> FetchResult:  # noqa: ANN001
    response = request_with_logging(session, "GET", spec.source_url, timeout=HTTP_TIMEOUT)
    response.raise_for_status()
    result = parse_ark_csv(response.text, spec.source_url)

    try:
        profile_response = request_with_logging(session, "GET", ARK_EXPENSE_PROFILE_URL, timeout=HTTP_TIMEOUT)
        profile_response.raise_for_status()
        profile = parse_ark_expense_profile_html(
            profile_response.text,
            symbol=spec.symbol,
            source_url=getattr(profile_response, "url", ARK_EXPENSE_PROFILE_URL),
        )
    except Exception:
        return result

    return FetchResult(
        as_of_date=result.as_of_date,
        source_url=result.source_url,
        source_format=result.source_format,
        rows=result.rows,
        profile=merge_profiles(profile, result.profile),
    )
