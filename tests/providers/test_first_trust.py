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
