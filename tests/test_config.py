from __future__ import annotations

from pathlib import Path

from etf_universe.cli import build_symbol_validator
from etf_universe.validation import AlpacaDataSymbolValidator


class DummySession:
    pass


def test_build_symbol_validator_reads_alpaca_credentials_from_dotenv(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ALPACA_DATA_API_KEY", raising=False)
    monkeypatch.delenv("ALPACA_DATA_SECRET_KEY", raising=False)
    monkeypatch.delenv("ALPACA_DATA_BASE_URL", raising=False)
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "ALPACA_DATA_API_KEY=test-key",
                "ALPACA_DATA_SECRET_KEY=test-secret",
                "ALPACA_DATA_BASE_URL=https://data.alpaca.markets",
                "",
            ]
        ),
        encoding="utf-8",
    )

    validator = build_symbol_validator(DummySession())

    assert isinstance(validator, AlpacaDataSymbolValidator)
    assert validator.enabled is True
    assert validator._api_key == "test-key"
    assert validator._secret_key == "test-secret"
