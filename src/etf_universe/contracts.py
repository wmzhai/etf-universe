from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date


@dataclass(frozen=True)
class EtfSpec:
    symbol: str
    group: str
    issuer: str
    provider: str
    source_url: str


@dataclass(frozen=True)
class SourceHoldingRow:
    constituent_symbol: str | None
    constituent_name: str | None
    weight: float | None
    asset_class: str | None = None
    security_type: str | None = None


@dataclass(frozen=True)
class EtfProfile:
    fundName: str | None = None
    exchange: str | None = None
    assetClass: str | None = None
    fundType: str | None = None
    cusip: str | None = None
    isin: str | None = None
    inceptionDate: str | None = None
    expenseRatio: float | None = None
    netExpenseRatio: float | None = None
    assetsUnderManagement: float | None = None
    sharesOutstanding: float | None = None
    distributionYield: float | None = None
    secYield30Day: float | None = None
    distributionFrequency: str | None = None
    profileAsOfDate: str | None = None
    profileSourceUrl: str | None = None


@dataclass(frozen=True)
class FetchResult:
    as_of_date: date
    source_url: str
    source_format: str
    rows: list[SourceHoldingRow]
    profile: EtfProfile = field(default_factory=EtfProfile)


@dataclass(frozen=True)
class NormalizedHoldingRow:
    symbol: str
    name: str | None
    weight: float | None


@dataclass(frozen=True)
class HoldingsMeta:
    etfSymbol: str
    issuer: str
    fetchedAt: str
    sourceUrl: str
    count: int
    fundName: str | None = None
    exchange: str | None = None
    assetClass: str | None = None
    cusip: str | None = None
    isin: str | None = None
    inceptionDate: str | None = None
    expenseRatio: float | None = None
    netExpenseRatio: float | None = None
    assetsUnderManagement: float | None = None
    sharesOutstanding: float | None = None
    distributionYield: float | None = None
    secYield30Day: float | None = None
