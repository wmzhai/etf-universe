from etf_universe.registry import ETF_SPECS, list_supported_symbols, parse_symbols_arg


def test_list_supported_symbols_is_sorted() -> None:
    assert list_supported_symbols() == sorted(ETF_SPECS)


def test_parse_symbols_arg_normalizes_order_and_dedupes() -> None:
    assert parse_symbols_arg(" qqq , spy, qqq ") == ["QQQ", "SPY"]
