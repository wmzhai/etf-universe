from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from etf_universe.cli import main
from etf_universe.contracts import EtfSpec, FetchResult, HoldingsMeta, NormalizedHoldingRow, SourceHoldingRow


class FakeValidator:
    def __init__(self, valid_symbols: set[str]) -> None:
        self.valid_symbols = valid_symbols
        self.calls: list[list[str]] = []

    def validate_symbols(self, symbols: list[str]) -> set[str]:
        self.calls.append(symbols)
        return self.valid_symbols


class FakeSession:
    def __init__(self) -> None:
        self.close_calls = 0

    def close(self) -> None:
        self.close_calls += 1


def test_holdings_fetch_writes_outputs_and_prints_summary(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    spec = EtfSpec(
        symbol="SPY",
        group="Layer 0",
        issuer="SSGA",
        provider="ssga",
        source_url="https://example.test/spy",
    )
    expected_fetch_result = FetchResult(
        as_of_date=date(2026, 3, 28),
        source_url=spec.source_url,
        source_format="xlsx",
        rows=[SourceHoldingRow("AAPL", "Apple Inc.", 6.1)],
    )
    normalized_rows = [NormalizedHoldingRow(symbol="AAPL", name="Apple Inc.", weight=6.1)]
    meta = HoldingsMeta(
        schemaVersion="2026-03-31.etf-universe-meta.v1",
        etfSymbol="SPY",
        issuer="SSGA",
        provider="ssga",
        asOfDate="2026-03-28",
        fetchedAt="2026-03-31T12:00:00Z",
        sourceUrl=spec.source_url,
        sourceFormat="xlsx",
        rowCount=1,
        normalizedRowCount=1,
        droppedRowCount=0,
    )

    session = FakeSession()
    validator = FakeValidator(valid_symbols={"AAPL"})
    writes: dict[str, Path] = {}

    def fake_make_session() -> FakeSession:
        return session

    def fake_build_symbol_validator(in_session: FakeSession) -> FakeValidator:
        assert in_session is session
        return validator

    def fake_fetch_with_provider(
        in_spec: EtfSpec,
        in_session: FakeSession,
        page: object | None = None,
    ) -> FetchResult:
        assert in_spec.symbol == "SPY"
        assert in_spec.provider == "ssga"
        assert in_spec.issuer == "SSGA"
        assert in_session is session
        assert page is None
        return expected_fetch_result

    def fake_collect_candidate_symbols(in_fetch_result: FetchResult) -> list[str]:
        assert in_fetch_result == expected_fetch_result
        return ["AAPL"]

    def fake_normalize_for_storage(
        spec: EtfSpec,
        fetched_at: datetime,
        fetch_result: FetchResult,
        valid_symbols: set[str],
    ) -> tuple[list[NormalizedHoldingRow], HoldingsMeta]:
        assert spec.symbol == "SPY"
        assert spec.provider == "ssga"
        assert spec.issuer == "SSGA"
        assert fetched_at.tzinfo == timezone.utc
        assert fetch_result == expected_fetch_result
        assert valid_symbols == {"AAPL"}
        return normalized_rows, meta

    def fake_write_parquet(rows: list[NormalizedHoldingRow], output_path: Path) -> None:
        assert rows == normalized_rows
        writes["parquet"] = output_path

    def fake_write_meta(in_meta: HoldingsMeta, output_path: Path) -> None:
        assert in_meta == meta
        writes["meta"] = output_path

    monkeypatch.setattr("etf_universe.cli.make_session", fake_make_session)
    monkeypatch.setattr("etf_universe.cli.build_symbol_validator", fake_build_symbol_validator)
    monkeypatch.setattr("etf_universe.cli.fetch_with_provider", fake_fetch_with_provider)
    monkeypatch.setattr("etf_universe.cli.collect_candidate_symbols", fake_collect_candidate_symbols)
    monkeypatch.setattr("etf_universe.cli.normalize_for_storage", fake_normalize_for_storage)
    monkeypatch.setattr("etf_universe.cli.write_parquet", fake_write_parquet)
    monkeypatch.setattr("etf_universe.cli.write_meta", fake_write_meta)

    exit_code = main(["holdings", "fetch", "--symbols", "SPY", "--output-dir", str(tmp_path)])

    assert exit_code == 0
    assert validator.calls == [["AAPL"]]
    assert session.close_calls == 1
    assert writes["parquet"] == tmp_path / "SPY.parquet"
    assert writes["meta"] == tmp_path / "SPY.meta.json"
    assert capsys.readouterr().out == "SPY: kept=1 dropped=0 as_of=2026-03-28 provider=SSGA\n"
