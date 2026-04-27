from __future__ import annotations

import csv
import io
import re
from datetime import date
from urllib.parse import urlsplit, urlunsplit

from bs4 import BeautifulSoup

from etf_universe.contracts import EtfProfile, EtfSpec, FetchResult, SourceHoldingRow
from etf_universe.normalization import clean_text, parse_date, parse_float
from etf_universe.profile import (
    label_value,
    label_value_with_as_of,
    merge_profiles,
    parse_compact_number,
    parse_profile_date,
)
from etf_universe.providers.base import HTTP_TIMEOUT, build_source_row, request_with_logging


def _product_page_url(source_url: str) -> str | None:
    parts = urlsplit(source_url)
    path = re.sub(r"/(?:fund/)?1467271812596\.ajax.*$", "", parts.path)
    if path == parts.path:
        return None
    return urlunsplit((parts.scheme, parts.netloc, path, "", ""))


def _parse_ishares_product_profile(html_text: str, source_url: str) -> EtfProfile:
    soup = BeautifulSoup(html_text, "html.parser")
    title = soup.title.get_text(" ", strip=True) if soup.title else None
    fund_name = clean_text(title.split("|", 1)[0]) if title else None
    lines = [line.strip() for line in soup.get_text("\n", strip=True).splitlines() if line.strip()]

    assets_under_management, profile_as_of_date = label_value_with_as_of(lines, "Net Assets of Fund")
    shares_outstanding, shares_as_of_date = label_value_with_as_of(lines, "Shares Outstanding")
    sec_yield, sec_yield_as_of_date = label_value_with_as_of(lines, "30 Day SEC Yield")
    distribution_yield, distribution_yield_as_of_date = label_value_with_as_of(
        lines,
        "12m Trailing Yield",
    )

    return EtfProfile(
        fundName=fund_name,
        exchange=label_value(lines, "Exchange"),
        assetClass=label_value(lines, "Asset Class"),
        cusip=label_value(lines, "CUSIP"),
        inceptionDate=parse_profile_date(label_value(lines, "Fund Inception")),
        expenseRatio=parse_float(label_value(lines, "Expense Ratio")),
        assetsUnderManagement=parse_compact_number(assets_under_management),
        sharesOutstanding=parse_compact_number(shares_outstanding),
        distributionYield=parse_float(distribution_yield),
        secYield30Day=parse_float(sec_yield),
        distributionFrequency=label_value(lines, "Distribution Frequency"),
        profileAsOfDate=(
            profile_as_of_date
            or shares_as_of_date
            or sec_yield_as_of_date
            or distribution_yield_as_of_date
        ),
        profileSourceUrl=source_url,
    )


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

    fund_name = clean_text(rows[0][0]) if rows and rows[0] else None
    inception_date = None
    shares_outstanding = None
    for row in rows[:header_idx]:
        if not row:
            continue
        label = clean_text(row[0])
        if label == "Inception Date" and len(row) > 1:
            inception_date = parse_profile_date(row[1])
        if label == "Shares Outstanding" and len(row) > 1:
            shares_outstanding = parse_compact_number(row[1])

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
        profile=EtfProfile(
            fundName=fund_name,
            inceptionDate=inception_date,
            sharesOutstanding=shares_outstanding,
            profileAsOfDate=as_of_date.isoformat(),
            profileSourceUrl=source_url,
        ),
    )


def fetch_ishares(spec: EtfSpec, session) -> FetchResult:  # noqa: ANN001
    response = request_with_logging(session, "GET", spec.source_url, timeout=HTTP_TIMEOUT)
    response.raise_for_status()
    result = parse_ishares_csv(response.text, spec.source_url)

    profile_url = _product_page_url(spec.source_url)
    if profile_url is None:
        return result

    try:
        profile_response = request_with_logging(session, "GET", profile_url, timeout=HTTP_TIMEOUT)
        profile_response.raise_for_status()
        page_profile = _parse_ishares_product_profile(profile_response.text, profile_url)
    except Exception:
        return result

    return FetchResult(
        as_of_date=result.as_of_date,
        source_url=result.source_url,
        source_format=result.source_format,
        rows=result.rows,
        profile=merge_profiles(page_profile, result.profile),
    )
