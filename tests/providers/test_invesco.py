from etf_universe.providers.invesco import build_rsp_api_url, parse_invesco_payload


def test_build_rsp_api_url_uses_isin_to_derive_cusip() -> None:
    assert build_rsp_api_url("US46137V3574").endswith(
        "/46137V357/holdings/fund?idType=cusip&productType=ETF"
    )


def test_parse_invesco_payload_extracts_holdings_rows() -> None:
    payload = {
        "effectiveDate": "2026-03-28",
        "holdings": [
            {
                "ticker": "AAPL",
                "issuerName": "Apple Inc.",
                "percentageOfTotalNetAssets": "6.1",
                "assetClassName": "Equity",
                "securityTypeName": "Common Stock",
            }
        ],
    }

    result = parse_invesco_payload(payload, "https://api.example.test/qqq")

    assert result.as_of_date.isoformat() == "2026-03-28"
    assert result.source_format == "json-browser"
    assert result.rows[0].constituent_symbol == "AAPL"
