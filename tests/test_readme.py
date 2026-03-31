from pathlib import Path
import re

from etf_universe.registry import list_supported_symbols


def test_supported_etfs_block_matches_registry() -> None:
    readme = Path("README.md").read_text(encoding="utf-8")
    start_marker = "<!-- supported-etfs:start -->"
    end_marker = "<!-- supported-etfs:end -->"

    assert start_marker in readme, "README missing supported-etfs:start marker"
    assert end_marker in readme, "README missing supported-etfs:end marker"

    block = readme.split(start_marker, 1)[1].split(end_marker, 1)[0]
    found_symbols = re.findall(r"`([A-Z0-9.-]+)`", block)

    expected = list_supported_symbols()
    assert found_symbols == expected, (
        "Supported ETF block symbols do not match registry list"
    )
