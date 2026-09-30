from __future__ import annotations

from typing import Any

from etf_universe.contracts import EtfSpec
from etf_universe.providers.vanguard import (
    fetch_vanguard,
    holdings_url,
    parse_vanguard_holdings,
    parse_vanguard_profile,
    profile_url,
)


class FakeResponse:
    def __init__(self, payload: dict[str, Any], url: str) -> None:
        self.payload = payload
        self.text = ""
        self.content = b"{}"
        self.status_code = 200
        self.url = url

    def json(self) -> dict[str, Any]:
        return self.payload

    def raise_for_status(self) -> None:
        pass


class FakeSession:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = responses
        self.calls: list[dict[str, Any]] = []

    def request(self, method: str, url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append({"method": method, "url": url, **kwargs})
        return self.responses.pop(0)


def _holdings_payload() -> dict[str, Any]:
    return {
        "holdingDetails": {
            "asOfDate": "08/31/2026",
            "equityHoldings": [
                {
                    "ticker": "NVDA",
                    "securityLongDescription": "NVIDIA Corp",
                    "marketValuePercentage": "6.87%",
                    "securityMainType": "EQ",
                    "securityType": "EQ.STOCK",
                },
                {
                    "ticker": "BRK/B",
                    "securityLongDescription": "Berkshire Hathaway Inc",
                    "marketValuePercentage": "1.23%",
                    "securityMainType": "EQ",
                    "securityType": "EQ.STOCK",
                },
                {
                    "ticker": "PLD",
                    "securityLongDescription": "Prologis Inc",
                    "marketValuePercentage": "0.18%",
                    "securityMainType": "EQ",
                    "securityType": "EQ.REIT",
                },
                {
                    "ticker": "ASPSW",
                    "securityLongDescription": "ALTISOURCE PORTFOLIO - 30",
                    "marketValuePercentage": "0.00%",
                    "securityMainType": "EQ",
                    "securityType": "EQ.WRT",
                },
            ],
            "shortTermReservesHoldings": [
                {
                    "securityLongDescription": "CASH",
                    "marketValuePercentage": "0.20%",
                    "securityMainType": "MM",
                    "securityType": "MM.CASH",
                }
            ],
        }
    }


def test_parse_vanguard_holdings_keeps_stock_and_reit_rows() -> None:
    result = parse_vanguard_holdings(_holdings_payload(), holdings_url("VTI"))

    assert result.source_format == "json"
    assert result.as_of_date.isoformat() == "2026-08-31"
    assert [row.constituent_symbol for row in result.rows] == ["NVDA", "BRK/B", "PLD"]
    assert result.rows[0].weight == 6.87
    assert result.rows[0].security_type == "EQ.STOCK"
    assert result.profile.profileAsOfDate == "2026-08-31"


def test_parse_vanguard_profile_extracts_share_class_facts() -> None:
    payload = {
        "dashboard": {
            "fundFullName": "Vanguard Total Stock Market ETF",
            "expenseRatio": "0.03%",
            "assetClass": "Domestic Stock - General",
            "secYield": "1.01%",
        },
        "overview": {
            "cusip": "922908769",
            "inceptionDate": "05/24/2001",
        },
        "portfolioComposition": {
            "characteristics": {
                "equityCharacteristic": {
                    "asOfDate": "08/31/2026",
                    "fund": {"shareClassTotalNetAssets": "$690.1 billion"},
                }
            }
        },
        "distributions": {"distributionSchedule": "Quarterly"},
    }

    profile = parse_vanguard_profile(payload, profile_url("VTI"))

    assert profile.fundName == "Vanguard Total Stock Market ETF"
    assert profile.cusip == "922908769"
    assert profile.inceptionDate == "2001-05-24"
    assert profile.expenseRatio == 0.03
    assert profile.assetsUnderManagement == 690_100_000_000.0
    assert profile.secYield30Day == 1.01
    assert profile.distributionFrequency == "Quarterly"
    assert profile.profileAsOfDate == "2026-08-31"


def test_fetch_vanguard_merges_holdings_and_profile() -> None:
    holdings_endpoint = holdings_url("VTI")
    profile_endpoint = profile_url("VTI")
    session = FakeSession([
        FakeResponse(_holdings_payload(), holdings_endpoint),
        FakeResponse(
            {
                "dashboard": {
                    "fundFullName": "Vanguard Total Stock Market ETF",
                    "expenseRatio": "0.03%",
                },
                "overview": {"cusip": "922908769"},
            },
            profile_endpoint,
        ),
    ])
    spec = EtfSpec(
        "VTI",
        "Layer 0",
        "Vanguard",
        "vanguard",
        "https://investor.vanguard.com/investment-products/etfs/profile/vti",
    )

    result = fetch_vanguard(spec, session)

    assert [call["url"] for call in session.calls] == [holdings_endpoint, profile_endpoint]
    assert result.source_url == holdings_endpoint
    assert result.profile.fundName == "Vanguard Total Stock Market ETF"
    assert result.profile.cusip == "922908769"
    assert result.profile.expenseRatio == 0.03
    assert result.profile.profileAsOfDate == "2026-08-31"
    assert [row.constituent_symbol for row in result.rows] == ["NVDA", "BRK/B", "PLD"]
