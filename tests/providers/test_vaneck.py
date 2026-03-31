import pytest

from etf_universe.providers.vaneck import extract_dataset_url, parse_vaneck_payload


def test_extract_dataset_url_finds_symbol_specific_json_endpoint() -> None:
    html_text = '<script>{"contentUrl":"https://api.example.test/holdings?ticker=SMH"}</script>'
    assert extract_dataset_url(html_text, "SMH") == "https://api.example.test/holdings?ticker=SMH"


def test_extract_dataset_url_allows_query_params_after_symbol() -> None:
    html_text = '<script>{"contentUrl":"https://api.example.test/holdings?foo=bar&ticker=SMH&baz=qux"}</script>'
    assert extract_dataset_url(html_text, "SMH") == "https://api.example.test/holdings?foo=bar&ticker=SMH&baz=qux"


def test_extract_dataset_url_rejects_similar_symbol_values() -> None:
    html_text = '<script>{"contentUrl":"https://api.example.test/holdings?ticker=SMHX&lang=en"}</script>'
    with pytest.raises(ValueError):
        extract_dataset_url(html_text, "SMH")


def test_parse_vaneck_payload_builds_fetch_result() -> None:
    payload = {
        "HoldingsList": [
            {
                "AsOfDate": "2026-03-28",
                "Holdings": [
                    {
                        "Label": "NVDA",
                        "HoldingName": "NVIDIA Corp.",
                        "Weight": "20.3",
                        "AssetClass": "Equity",
                        "SecurityType": "Common Stock",
                    },
                    {
                        "Label": "TSM",
                        "HoldingName": "Taiwan Semiconductor",
                        "Weight": "11.2",
                        "AssetClass": "Equity",
                        "SecurityType": None,
                        "Classification": "Common Stock",
                    },
                ],
            }
        ]
    }

    result = parse_vaneck_payload(payload, "https://api.example.test/holdings?ticker=SMH")

    assert result.source_format == "json"
    assert result.as_of_date.isoformat() == "2026-03-28"
    assert [row.constituent_symbol for row in result.rows] == ["NVDA", "TSM"]
    assert [row.security_type for row in result.rows] == ["Common Stock", "Common Stock"]
