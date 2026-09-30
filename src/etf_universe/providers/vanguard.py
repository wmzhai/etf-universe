from __future__ import annotations

from typing import Any

from etf_universe.contracts import EtfProfile, EtfSpec, FetchResult, SourceHoldingRow
from etf_universe.normalization import clean_text, parse_date, parse_float
from etf_universe.profile import merge_profiles, parse_compact_number, parse_profile_date
from etf_universe.providers.base import HTTP_TIMEOUT, build_source_row, request_with_logging


HOLDINGS_URL = "https://investor.vanguard.com/irr/funds/profile/{symbol}-AdditionalFundData"
PROFILE_URL = "https://investor.vanguard.com/irr/funds/profile/{symbol}"
EQUITY_SECURITY_TYPES = frozenset({"EQ.STOCK", "EQ.REIT"})


def holdings_url(symbol: str) -> str:
    return HOLDINGS_URL.format(symbol=symbol)


def profile_url(symbol: str) -> str:
    return PROFILE_URL.format(symbol=symbol)


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {}


def parse_vanguard_holdings(payload: dict[str, Any], source_url: str) -> FetchResult:
    details = _mapping(payload.get("holdingDetails"))
    if not details:
        raise ValueError("Vanguard holdings response has no holdingDetails")

    as_of_date = parse_date(details.get("asOfDate"))
    equity_rows = details.get("equityHoldings")
    if not isinstance(equity_rows, list):
        raise ValueError("Vanguard holdings response has no equityHoldings")

    records: list[SourceHoldingRow] = []
    for raw_row in equity_rows:
        row = _mapping(raw_row)
        security_type = clean_text(row.get("securityType"))
        if security_type not in EQUITY_SECURITY_TYPES:
            continue
        symbol = row.get("ticker")
        name = row.get("securityLongDescription") or row.get("securityShortDescription")
        if clean_text(symbol) is None and clean_text(name) is None:
            continue
        records.append(
            build_source_row(
                constituent_symbol=symbol,
                constituent_name=name,
                weight=row.get("marketValuePercentage"),
                asset_class=row.get("securityMainType"),
                security_type=security_type,
            )
        )

    if not records:
        raise ValueError("Vanguard holdings response has no equity rows")

    return FetchResult(
        as_of_date=as_of_date,
        source_url=source_url,
        source_format="json",
        rows=records,
        profile=EtfProfile(
            profileAsOfDate=as_of_date.isoformat(),
            profileSourceUrl=source_url,
        ),
    )


def parse_vanguard_profile(payload: dict[str, Any], source_url: str) -> EtfProfile:
    dashboard = _mapping(payload.get("dashboard"))
    overview = _mapping(payload.get("overview"))
    distributions = _mapping(payload.get("distributions"))
    composition = _mapping(payload.get("portfolioComposition"))
    characteristics = _mapping(composition.get("characteristics"))
    equity = _mapping(characteristics.get("equityCharacteristic"))
    fund = _mapping(equity.get("fund"))

    return EtfProfile(
        fundName=clean_text(dashboard.get("fundFullName")),
        assetClass=clean_text(dashboard.get("assetClass")),
        cusip=clean_text(overview.get("cusip")),
        inceptionDate=parse_profile_date(overview.get("inceptionDate")),
        expenseRatio=parse_float(dashboard.get("expenseRatio")),
        assetsUnderManagement=parse_compact_number(fund.get("shareClassTotalNetAssets")),
        secYield30Day=parse_float(dashboard.get("secYield")),
        distributionFrequency=clean_text(distributions.get("distributionSchedule")),
        profileAsOfDate=parse_profile_date(equity.get("asOfDate")),
        profileSourceUrl=source_url,
    )


def fetch_vanguard(spec: EtfSpec, session) -> FetchResult:  # noqa: ANN001
    holdings_endpoint = holdings_url(spec.symbol)
    response = request_with_logging(session, "GET", holdings_endpoint, timeout=HTTP_TIMEOUT)
    response.raise_for_status()
    result = parse_vanguard_holdings(response.json(), holdings_endpoint)

    try:
        profile_endpoint = profile_url(spec.symbol)
        profile_response = request_with_logging(
            session,
            "GET",
            profile_endpoint,
            timeout=HTTP_TIMEOUT,
        )
        profile_response.raise_for_status()
        profile = parse_vanguard_profile(profile_response.json(), profile_endpoint)
    except Exception:
        return result

    return FetchResult(
        as_of_date=result.as_of_date,
        source_url=result.source_url,
        source_format=result.source_format,
        rows=result.rows,
        profile=merge_profiles(profile, result.profile),
    )
