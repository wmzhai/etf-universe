import pytest

from etf_universe.providers.first_trust import parse_first_trust_html, parse_first_trust_profile_html


def test_parse_first_trust_html_targets_fund_silver_grid() -> None:
    html_text = """
    <html>
      <body>
        <span id="ctl00_MainContent_lblHoldingsTitle">Holdings as of Mar 28, 2026</span>
        <table class="fundSilverGrid">
          <tr>
            <th>Security Name</th><th>Ticker</th><th>Sector</th><th>Classification</th><th>X</th><th>Y</th><th>Weighting</th>
          </tr>
          <tr>
            <td>Amazon.com Inc.</td><td>AMZN</td><td>Consumer</td><td>Common Stock</td><td></td><td></td><td>5.1%</td>
          </tr>
        </table>
      </body>
    </html>
    """

    result = parse_first_trust_html(html_text, "https://example.com/fdn")

    assert result.as_of_date.isoformat() == "2026-03-28"
    assert result.rows[0].constituent_symbol == "AMZN"


def test_parse_first_trust_html_handles_live_header_layout() -> None:
    html_text = """
    <html>
      <body>
        <span id="ctl00_MainContent_lblHoldingsTitle">Holdings as of Mar 28, 2026</span>
        <table class="fundSilverGrid">
          <tr>
            <th>Security Name</th><th>Identifier</th><th>CUSIP</th><th>Classification</th><th>Shares / Quantity</th><th>Market Value</th><th>Weighting</th>
          </tr>
          <tr>
            <td>Amazon.com, Inc.</td><td>AMZN</td><td>023135106</td><td>Consumer Discretionary</td><td>2,245,205</td><td>$451,173,944.75</td><td>10.23%</td>
          </tr>
        </table>
      </body>
    </html>
    """

    result = parse_first_trust_html(html_text, "https://example.com/fdn")

    row = result.rows[0]
    assert row.constituent_symbol == "AMZN"
    assert row.constituent_name == "Amazon.com, Inc."
    assert row.security_type == "Consumer Discretionary"
    assert row.weight == 10.23


def test_parse_first_trust_html_raises_if_required_headers_missing() -> None:
    html_text = """
    <html>
      <body>
        <span id="ctl00_MainContent_lblHoldingsTitle">Holdings as of Mar 28, 2026</span>
        <table class="fundSilverGrid">
          <tr>
            <th>Security Name</th><th>Ticker</th><th>X</th><th>Y</th><th>Weighting</th>
          </tr>
          <tr>
            <td>Amazon.com Inc.</td><td>AMZN</td><td></td><td></td><td>5.1%</td>
          </tr>
        </table>
      </body>
    </html>
    """

    with pytest.raises(ValueError):
        parse_first_trust_html(html_text, "https://example.com/fdn")


def test_parse_first_trust_profile_html_extracts_fund_details() -> None:
    html_text = """
    <html>
      <head><title>First Trust Dow Jones Internet Index Fund (FDN)</title></head>
      <body>
        <table>
          <tr><td>Fund Type</td><td>Internet</td></tr>
          <tr><td>CUSIP</td><td>33733E302</td></tr>
          <tr><td>ISIN</td><td>US33733E3027</td></tr>
          <tr><td>Exchange</td><td>NYSE Arca</td></tr>
          <tr><td>Inception</td><td>6/19/2006</td></tr>
          <tr><td>Total Expense Ratio*</td><td>0.49%</td></tr>
          <tr><td>Net Expense Ratio*</td><td>0.49%</td></tr>
          <tr><td>Total Net Assets</td><td>$5,236,185,125</td></tr>
          <tr><td>Outstanding Shares</td><td>19,900,002</td></tr>
        </table>
      </body>
    </html>
    """

    profile = parse_first_trust_profile_html(html_text, "https://example.com/fdn-summary")

    assert profile.fundName == "First Trust Dow Jones Internet Index Fund"
    assert profile.exchange == "NYSE Arca"
    assert profile.fundType == "Internet"
    assert profile.cusip == "33733E302"
    assert profile.isin == "US33733E3027"
    assert profile.inceptionDate == "2006-06-19"
    assert profile.expenseRatio == 0.49
    assert profile.netExpenseRatio == 0.49
    assert profile.assetsUnderManagement == 5236185125.0
    assert profile.sharesOutstanding == 19900002.0


def test_parse_first_trust_profile_html_does_not_guess_summary_values_from_page_text() -> None:
    html_text = """
    <html>
      <head><title>First Trust Dow Jones Internet Index Fund (FDN)</title></head>
      <body>
        <h2>CUSIP</h2>
        <a>UIT Tax Center</a>
        <h2>Exchange</h2>
        <a>Quantitative Stock Selection White Paper (PDF)</a>
      </body>
    </html>
    """

    profile = parse_first_trust_profile_html(html_text, "https://example.com/fdn-summary")

    assert profile.cusip is None
    assert profile.exchange is None
