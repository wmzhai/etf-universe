from etf_universe.providers.ishares import parse_ishares_csv


def test_parse_ishares_csv_extracts_as_of_date_and_rows() -> None:
    csv_text = """Fund Holdings as of,Mar 28, 2026
Ticker,Name,Sector,Asset Class,Weight (%),Security Type
AAPL,Apple Inc.,Technology,Equity,6.10,Common Stock
MSFT,Microsoft Corp.,Technology,Equity,5.90,Common Stock
"""

    result = parse_ishares_csv(csv_text, "https://example.com/iwm.csv")

    assert result.source_format == "csv"
    assert result.as_of_date.isoformat() == "2026-03-28"
    assert [row.constituent_symbol for row in result.rows] == ["AAPL", "MSFT"]
