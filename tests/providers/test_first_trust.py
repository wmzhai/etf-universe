import pytest

from etf_universe.providers.first_trust import parse_first_trust_html


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
