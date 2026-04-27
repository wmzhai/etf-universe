from __future__ import annotations

from typing import Any

from etf_universe.providers.ark import fetch_ark, parse_ark_csv, parse_ark_expense_profile_html
from etf_universe.contracts import EtfSpec


class FakeResponse:
    def __init__(self, text: str, url: str = "https://example.com") -> None:
        self.text = text
        self.content = text.encode()
        self.status_code = 200
        self.url = url

    def raise_for_status(self) -> None:
        pass


class FakeSession:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = responses
        self.calls: list[dict[str, Any]] = []

    def request(self, method: str, url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append({"method": method, "url": url, **kwargs})
        return self.responses.pop(0)


def test_parse_ark_csv_extracts_as_of_date_and_rows() -> None:
    csv_text = """date,fund,company,ticker,cusip,shares,market value ($),weight (%)
04/27/2026,ARKK,TESLA INC,TSLA,88160R101,"2,526,013","$950,538,691.90",9.71%
04/27/2026,ARKK,ADVANCED MICRO DEVICES,AMD,007903107,"1,591,290","$553,466,574.90",5.65%
"""

    result = parse_ark_csv(csv_text, "https://example.com/arkk.csv")

    assert result.source_format == "csv"
    assert result.as_of_date.isoformat() == "2026-04-27"
    assert [row.constituent_symbol for row in result.rows] == ["TSLA", "AMD"]
    assert [row.constituent_name for row in result.rows] == [
        "TESLA INC",
        "ADVANCED MICRO DEVICES",
    ]
    assert [row.weight for row in result.rows] == [9.71, 5.65]
    assert result.profile is not None
    assert result.profile.fundName == "ARK Innovation ETF"
    assert result.profile.assetClass == "Equity"
    assert result.profile.profileAsOfDate == "2026-04-27"


def test_parse_ark_expense_profile_html_extracts_default_and_arkw_exception() -> None:
    html_text = """
    <html>
      <body>
        The annual expense ratio (or management fee) of each of ARK's actively managed ETFs
        is 0.75%, except for ARKW which is 0.88%.
      </body>
    </html>
    """

    arkk_profile = parse_ark_expense_profile_html(
        html_text,
        symbol="ARKK",
        source_url="https://helpcenter.ark-funds.com/fees",
    )
    arkw_profile = parse_ark_expense_profile_html(
        html_text,
        symbol="ARKW",
        source_url="https://helpcenter.ark-funds.com/fees",
    )

    assert arkk_profile.expenseRatio == 0.75
    assert arkw_profile.expenseRatio == 0.88
    assert arkk_profile.profileSourceUrl == "https://helpcenter.ark-funds.com/fees"


def test_fetch_ark_merges_expense_ratio_profile() -> None:
    csv_text = """date,fund,company,ticker,cusip,shares,market value ($),weight (%)
04/27/2026,ARKW,TESLA INC,TSLA,88160R101,"2,526,013","$950,538,691.90",9.71%
"""
    profile_html = """
    <html>
      <body>
        The annual expense ratio (or management fee) of each of ARK's actively managed ETFs
        is 0.75%, except for ARKW which is 0.88%.
      </body>
    </html>
    """
    session = FakeSession([
        FakeResponse(csv_text, "https://assets.ark-funds.com/arkw.csv"),
        FakeResponse(profile_html, "https://helpcenter.ark-funds.com/fees"),
    ])
    spec = EtfSpec(
        "ARKW",
        "Layer 2",
        "ARK",
        "ark",
        "https://assets.ark-funds.com/fund-documents/funds-etf-csv/ARK_NEXT_GENERATION_INTERNET_ETF_ARKW_HOLDINGS.csv",
    )

    result = fetch_ark(spec, session)

    assert result.profile.fundName == "ARK Next Generation Internet ETF"
    assert result.profile.expenseRatio == 0.88
    assert result.profile.profileAsOfDate == "2026-04-27"
    assert result.profile.profileSourceUrl == "https://helpcenter.ark-funds.com/fees"
    assert [call["url"] for call in session.calls] == [
        "https://assets.ark-funds.com/fund-documents/funds-etf-csv/ARK_NEXT_GENERATION_INTERNET_ETF_ARKW_HOLDINGS.csv",
        "https://helpcenter.ark-funds.com/what-is-the-fee-structure-expense-ratio-of-ark-etfs",
    ]


def test_parse_ark_csv_rejects_mixed_as_of_dates() -> None:
    csv_text = """date,fund,company,ticker,cusip,shares,market value ($),weight (%)
04/27/2026,ARKK,TESLA INC,TSLA,88160R101,"2,526,013","$950,538,691.90",9.71%
04/26/2026,ARKK,ADVANCED MICRO DEVICES,AMD,007903107,"1,591,290","$553,466,574.90",5.65%
"""

    try:
        parse_ark_csv(csv_text, "https://example.com/arkk.csv")
    except ValueError as exc:
        assert str(exc) == "ARK CSV contains mixed as-of dates"
    else:
        raise AssertionError("Expected mixed ARK dates to be rejected")
