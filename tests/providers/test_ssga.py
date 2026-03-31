from io import BytesIO

from openpyxl import Workbook

from etf_universe.providers.ssga import parse_ssga_workbook


def test_parse_ssga_workbook_extracts_holdings_rows() -> None:
    workbook = Workbook()
    sheet = workbook.active
    sheet.append([])
    sheet.append([])
    sheet.append(["As of Mar 28, 2026", None])
    sheet.append([])
    sheet.append(["Ticker", "Name", "Weight"])
    sheet.append(["AAPL", "Apple Inc.", "6.1"])
    sheet.append(["BRK/B", "Berkshire Hathaway Inc.", "1.9"])

    buffer = BytesIO()
    workbook.save(buffer)

    result = parse_ssga_workbook(buffer.getvalue(), "https://example.com/spy.xlsx")

    assert result.source_format == "xlsx"
    assert result.as_of_date.isoformat() == "2026-03-28"
    assert [row.constituent_symbol for row in result.rows] == ["AAPL", "BRK/B"]
