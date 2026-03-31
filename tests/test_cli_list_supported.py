from etf_universe.cli import main


def test_holdings_list_supported_prints_one_symbol_per_line(capsys) -> None:
    exit_code = main(["holdings", "list-supported"])

    assert exit_code == 0
    lines = capsys.readouterr().out.strip().splitlines()
    assert lines == sorted(lines)
    assert "SPY" in lines
    assert "QQQ" in lines
