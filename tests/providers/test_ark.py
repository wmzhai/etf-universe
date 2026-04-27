from etf_universe.providers.ark import parse_ark_csv


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
