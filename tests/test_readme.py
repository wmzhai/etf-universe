from dataclasses import fields
from pathlib import Path
import re

from etf_universe.cli import DEFAULT_OUTPUT_DIR
from etf_universe.contracts import HoldingsMeta
from etf_universe.registry import list_supported_symbols
from etf_universe.storage import PARQUET_SCHEMA


README_PATH = Path(__file__).resolve().parents[1] / "README.md"


def readme_text() -> str:
    return README_PATH.read_text(encoding="utf-8")


def _assert_readme_contains_symbol(run_command: str) -> None:
    readme = readme_text()
    assert run_command in readme, f"README must advertise `{run_command}`"


def test_supported_etfs_block_matches_registry() -> None:
    readme = readme_text()
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


def test_fetch_example_includes_comma_separated_symbols() -> None:
    _assert_readme_contains_symbol(
        "uv run etf-universe fetch --symbols SPY,XLK --output-dir ./output"
    )


def test_list_example_uses_flat_command() -> None:
    _assert_readme_contains_symbol("uv run etf-universe list")


def test_readme_mentions_default_output_dir() -> None:
    expected = f"defaults to `{DEFAULT_OUTPUT_DIR}`"
    assert expected in readme_text(), f"README should mention `{expected}`"


def test_readme_documents_alpaca_validation_and_env_sample() -> None:
    readme = readme_text()
    assert "Alpaca" in readme
    assert ".env.sample" in readme
    assert "ALPACA_DATA_API_KEY" in readme
    assert "ALPACA_DATA_SECRET_KEY" in readme


def test_cli_default_output_dir_matches_expected_location() -> None:
    assert DEFAULT_OUTPUT_DIR == Path("output")


def test_readme_documents_parquet_schema() -> None:
    readme = readme_text()
    for column in PARQUET_SCHEMA.names:
        assert (
            f"`{column}`" in readme
        ), f"README should list `{column}` as a parquet column or field"


def test_metadata_fields_documented() -> None:
    readme = readme_text()
    for field in fields(HoldingsMeta):
        assert field.name in readme, (
            f"README should mention metadata field `{field.name}`"
        )
