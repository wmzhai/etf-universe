from __future__ import annotations

from dataclasses import dataclass
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
class FetchResult:
    as_of_date: date
    source_url: str
    source_format: str
    rows: list[SourceHoldingRow]


@dataclass(frozen=True)
class NormalizedHoldingRow:
    symbol: str
    name: str | None
    weight: float | None


@dataclass(frozen=True)
class HoldingsMeta:
    schemaVersion: str
    etfSymbol: str
    issuer: str
    provider: str
    asOfDate: str
    fetchedAt: str
    sourceUrl: str
    sourceFormat: str
    rowCount: int
    normalizedRowCount: int
    droppedRowCount: int
