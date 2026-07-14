from __future__ import annotations

from typing import Any

from etf_universe.contracts import EtfSpec
from etf_universe.providers.ishares import (
    _parse_ishares_product_profile,
    fetch_ishares,
    parse_ishares_csv,
)


class FakeResponse:
    def __init__(self, text: str, url: str) -> None:
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


def test_parse_ishares_csv_extracts_as_of_date_and_rows() -> None:
    csv_text = """iShares Semiconductor ETF
Fund Holdings as of,Mar 28, 2026
Inception Date,"Jul 10, 2001"
Shares Outstanding,"65,900,000.00"
Ticker,Name,Sector,Asset Class,Weight (%),Security Type
AAPL,Apple Inc.,Technology,Equity,6.10,Common Stock
MSFT,Microsoft Corp.,Technology,Equity,5.90,Common Stock
"""

    result = parse_ishares_csv(csv_text, "https://example.com/iwm.csv")

    assert result.source_format == "csv"
    assert result.as_of_date.isoformat() == "2026-03-28"
    assert [row.constituent_symbol for row in result.rows] == ["AAPL", "MSFT"]
    assert result.profile is not None
    assert result.profile.fundName == "iShares Semiconductor ETF"
    assert result.profile.inceptionDate == "2001-07-10"
    assert result.profile.sharesOutstanding == 65900000.0


def test_parse_ishares_csv_skips_non_equity_rows() -> None:
    csv_text = """Fund Holdings as of,Mar 28, 2026
Ticker,Name,Sector,Asset Class,Weight (%),Security Type
AAPL,Apple Inc.,Technology,Equity,6.10,Common Stock
RTYM6,RUSSELL 2000 EMINI CME JUN 26,Derivatives,Futures,0.00,
MSFT,Microsoft Corp.,Technology,Equity,5.90,Common Stock
"""

    result = parse_ishares_csv(csv_text, "https://example.com/iwm.csv")

    assert [row.constituent_symbol for row in result.rows] == ["AAPL", "MSFT"]


def test_parse_ishares_product_profile_extracts_distribution_yield() -> None:
    html_text = """
    <html>
      <head><title>iShares Semiconductor ETF | iShares</title></head>
      <body>
        <dl>
          <dt>30 Day SEC Yield</dt>
          <dd>as of Mar 31, 2026</dd>
          <dd>0.27%</dd>
          <dt>12m Trailing Yield</dt>
          <dd>as of Mar 31, 2026</dd>
          <dd>0.51%</dd>
          <dt>Distribution Frequency</dt>
          <dd>Quarterly</dd>
        </dl>
      </body>
    </html>
    """

    profile = _parse_ishares_product_profile(html_text, "https://example.com/soxx")

    assert profile.secYield30Day == 0.27
    assert profile.distributionYield == 0.51
    assert profile.distributionFrequency == "Quarterly"


def test_fetch_ishares_uses_current_holdings_endpoint_and_product_page() -> None:
    source_url = (
        "https://www.ishares.com/us/products/239710/ishares-russell-2000-etf/"
        "1467271812596.ajax?fileType=csv"
    )
    holdings_url = (
        "https://www.blackrock.com/varnish-api/blk-one01-product-data/product-data/api/v1/"
        "get-fund-document?appType=PRODUCT_PAGE&appSubType=ISHARES&targetSite=us-ishares&"
        "locale=en_US&portfolioId=239710&userType=individual&component=holdings"
    )
    product_url = "https://www.ishares.com/us/products/239710/ishares-russell-2000-etf"
    csv_text = """iShares Russell 2000 ETF
Fund Holdings as of,Jul 10, 2026
Ticker,Name,Sector,Asset Class,Weight (%),Security Type
AAPL,Apple Inc.,Technology,Equity,6.10,Common Stock
"""
    profile_html = """
    <html>
      <head><title>iShares Russell 2000 ETF | iShares</title></head>
      <body><dl><dt>Expense Ratio</dt><dd>0.19%</dd></dl></body>
    </html>
    """
    session = FakeSession([
        FakeResponse(csv_text, holdings_url),
        FakeResponse(profile_html, product_url),
    ])
    spec = EtfSpec("IWM", "Layer 0", "iShares", "ishares", source_url)

    result = fetch_ishares(spec, session)

    assert result.source_url == holdings_url
    assert result.profile.fundName == "iShares Russell 2000 ETF"
    assert result.profile.expenseRatio == 0.19
    assert [call["url"] for call in session.calls] == [holdings_url, product_url]
