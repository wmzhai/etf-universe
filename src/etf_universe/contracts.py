from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class EtfSpec:
    symbol: str
    group: str
    issuer: str
    provider: str
    source_url: str
