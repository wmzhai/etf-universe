from etf_universe.providers.ishares import _parse_ishares_product_profile, parse_ishares_csv


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
