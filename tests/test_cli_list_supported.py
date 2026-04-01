from etf_universe.cli import main
from etf_universe.registry import list_supported_symbols


def test_list_prints_one_symbol_per_line(capsys) -> None:
    exit_code = main(["list"])

    assert exit_code == 0
    assert capsys.readouterr().out == "".join(f"{symbol}\n" for symbol in list_supported_symbols())
