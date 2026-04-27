from etf_universe.registry import ETF_SPECS, list_supported_symbols, parse_symbols_arg


def test_list_supported_symbols_is_sorted() -> None:
    assert list_supported_symbols() == sorted(ETF_SPECS)


def test_parse_symbols_arg_normalizes_order_and_dedupes() -> None:
    assert parse_symbols_arg(" qqq , spy, qqq ") == ["QQQ", "SPY"]


def test_thematic_etfs_are_registered() -> None:
    expected_specs = {
        "SOXX": ("Layer 2", "iShares", "ishares"),
        "ARKK": ("Layer 2", "ARK", "ark"),
        "ARKG": ("Layer 2", "ARK", "ark"),
        "ARKW": ("Layer 2", "ARK", "ark"),
        "ARKQ": ("Layer 2", "ARK", "ark"),
        "ARKF": ("Layer 2", "ARK", "ark"),
        "ARKX": ("Layer 2", "ARK", "ark"),
    }

    for symbol, (group, issuer, provider) in expected_specs.items():
        spec = ETF_SPECS[symbol]
        assert spec.group == group
        assert spec.issuer == issuer
        assert spec.provider == provider
        assert spec.source_url.startswith("https://")

    assert ETF_SPECS["ARKQ"].source_url.endswith(
        "ARK_AUTONOMOUS_TECH._&_ROBOTICS_ETF_ARKQ_HOLDINGS.csv"
    )
