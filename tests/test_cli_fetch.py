from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
import threading
import time

import pytest

from etf_universe.cli import DEFAULT_OUTPUT_DIR, main
from etf_universe.contracts import EtfSpec, FetchResult, HoldingsMeta, NormalizedHoldingRow, SourceHoldingRow
from etf_universe.registry import list_supported_symbols


class FakeValidator:
    def __init__(self, valid_symbols: set[str]) -> None:
        self.valid_symbols = valid_symbols
        self.calls: list[list[str]] = []

    def validate_symbols(self, symbols: list[str]) -> set[str]:
        self.calls.append(symbols)
        return self.valid_symbols


class FakeSession:
    def __init__(self, name: str = "session") -> None:
        self.name = name
        self.close_calls = 0

    def close(self) -> None:
        self.close_calls += 1


def test_fetch_multi_etf_uses_browser_and_validates_symbols_once(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    spy_spec = EtfSpec(
        symbol="SPY",
        group="Layer 0",
        issuer="SSGA",
        provider="ssga",
        source_url="https://example.test/spy",
    )
    qqq_spec = EtfSpec(
        symbol="QQQ",
        group="Layer 0",
        issuer="Invesco",
        provider="invesco",
        source_url="https://example.test/qqq",
    )
    spy_fetch_result = FetchResult(
        as_of_date=date(2026, 3, 28),
        source_url=spy_spec.source_url,
        source_format="xlsx",
        rows=[SourceHoldingRow("AAPL", "Apple Inc.", 6.1)],
    )
    qqq_fetch_result = FetchResult(
        as_of_date=date(2026, 3, 28),
        source_url=qqq_spec.source_url,
        source_format="json-browser",
        rows=[SourceHoldingRow("NVDA", "NVIDIA Corp.", 6.0)],
    )
    spy_rows = [NormalizedHoldingRow(symbol="AAPL", name="Apple Inc.", weight=6.1)]
    qqq_rows = [NormalizedHoldingRow(symbol="NVDA", name="NVIDIA Corp.", weight=6.0)]
    spy_meta = HoldingsMeta(
        etfSymbol="SPY",
        issuer="SSGA",
        provider="ssga",
        asOfDate="2026-03-28",
        fetchedAt="2026-03-31T12:00:00Z",
        sourceUrl=spy_spec.source_url,
        sourceFormat="xlsx",
        rowCount=1,
        normalizedRowCount=1,
        droppedRowCount=0,
    )
    qqq_meta = HoldingsMeta(
        etfSymbol="QQQ",
        issuer="Invesco",
        provider="invesco",
        asOfDate="2026-03-28",
        fetchedAt="2026-03-31T12:00:00Z",
        sourceUrl=qqq_spec.source_url,
        sourceFormat="json-browser",
        rowCount=1,
        normalizedRowCount=1,
        droppedRowCount=0,
    )

    session = FakeSession()
    validator = FakeValidator(valid_symbols={"AAPL", "MSFT", "NVDA"})
    writes: list[tuple[str, Path]] = []
    page_token = object()
    launch_calls = 0
    close_calls = 0
    fetch_calls: list[tuple[str, object | None]] = []

    def fake_make_session() -> FakeSession:
        return session

    def fake_build_symbol_validator(in_session: FakeSession) -> FakeValidator:
        assert in_session is session
        return validator

    def fake_launch_browser() -> tuple[object, object, object]:
        nonlocal launch_calls
        launch_calls += 1
        return (object(), object(), page_token)

    def fake_close_browser(_playwright: object, _browser: object) -> None:
        nonlocal close_calls
        close_calls += 1

    def fake_fetch_with_provider(
        in_spec: EtfSpec,
        in_session: FakeSession,
        page: object | None = None,
    ) -> FetchResult:
        assert in_session is session
        fetch_calls.append((in_spec.symbol, page))
        if in_spec.symbol == "SPY":
            return spy_fetch_result
        if in_spec.symbol == "QQQ":
            assert page is page_token
            return qqq_fetch_result
        raise AssertionError(f"unexpected symbol: {in_spec.symbol}")

    def fake_collect_candidate_symbols(in_fetch_result: FetchResult) -> list[str]:
        if in_fetch_result == spy_fetch_result:
            return ["AAPL", "MSFT"]
        if in_fetch_result == qqq_fetch_result:
            return ["MSFT", "NVDA"]
        raise AssertionError("unexpected fetch result")

    def fake_normalize_for_storage(
        spec: EtfSpec,
        fetched_at: datetime,
        fetch_result: FetchResult,
        valid_symbols: set[str],
    ) -> tuple[list[NormalizedHoldingRow], HoldingsMeta]:
        assert fetched_at.tzinfo == timezone.utc
        assert valid_symbols == {"AAPL", "MSFT", "NVDA"}
        if spec.symbol == "SPY":
            assert fetch_result == spy_fetch_result
            return spy_rows, spy_meta
        if spec.symbol == "QQQ":
            assert fetch_result == qqq_fetch_result
            return qqq_rows, qqq_meta
        raise AssertionError(f"unexpected symbol: {spec.symbol}")

    def fake_write_parquet(rows: list[NormalizedHoldingRow], output_path: Path) -> None:
        writes.append(("parquet", output_path))
        assert rows in (spy_rows, qqq_rows)

    def fake_write_meta(in_meta: HoldingsMeta, output_path: Path) -> None:
        writes.append(("meta", output_path))
        assert in_meta in (spy_meta, qqq_meta)

    monkeypatch.setattr("etf_universe.cli.make_session", fake_make_session)
    monkeypatch.setattr("etf_universe.cli.build_symbol_validator", fake_build_symbol_validator)
    monkeypatch.setattr("etf_universe.cli.launch_browser", fake_launch_browser)
    monkeypatch.setattr("etf_universe.cli.close_browser", fake_close_browser)
    monkeypatch.setattr("etf_universe.cli.fetch_with_provider", fake_fetch_with_provider)
    monkeypatch.setattr("etf_universe.cli.collect_candidate_symbols", fake_collect_candidate_symbols)
    monkeypatch.setattr("etf_universe.cli.normalize_for_storage", fake_normalize_for_storage)
    monkeypatch.setattr("etf_universe.cli.write_parquet", fake_write_parquet)
    monkeypatch.setattr("etf_universe.cli.write_meta", fake_write_meta)

    exit_code = main(["fetch", "--symbols", "SPY,QQQ", "--output-dir", str(tmp_path)])

    assert exit_code == 0
    assert launch_calls == 1
    assert close_calls == 1
    assert fetch_calls == [("SPY", None), ("QQQ", page_token)]
    assert validator.calls == [["AAPL", "MSFT", "MSFT", "NVDA"]]
    assert session.close_calls == 1
    assert writes == [
        ("parquet", tmp_path / "SPY.parquet"),
        ("meta", tmp_path / "SPY.meta.json"),
        ("parquet", tmp_path / "QQQ.parquet"),
        ("meta", tmp_path / "QQQ.meta.json"),
    ]
    captured = capsys.readouterr()
    assert (
        captured.out
        == "SPY: kept=1 dropped=0 as_of=2026-03-28 provider=SSGA\n"
        "QQQ: kept=1 dropped=0 as_of=2026-03-28 provider=Invesco\n"
    )
    assert "event=fetch.start" in captured.err
    assert "symbol_count=2" in captured.err
    assert f"output_dir={tmp_path}" in captured.err
    assert "event=provider.fetch.start" in captured.err
    assert "etf=SPY" in captured.err
    assert "etf=QQQ" in captured.err
    assert "event=provider.fetch.done" in captured.err
    assert "row_count=1" in captured.err
    assert "event=validation.start" in captured.err
    assert "candidate_count=4" in captured.err
    assert "event=validation.done" in captured.err
    assert "valid_count=3" in captured.err
    assert "event=phase.done" in captured.err
    assert "stage=fetch" in captured.err
    assert "stage=write" in captured.err
    assert "event=storage.write.done" in captured.err
    assert "parquet_path=" in captured.err
    assert "meta_path=" in captured.err
    assert "event=fetch.done" in captured.err
    assert "etf_count=2" in captured.err


def test_fetch_runs_non_invesco_specs_concurrently(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    spy_spec = EtfSpec(
        symbol="SPY",
        group="Layer 0",
        issuer="SSGA",
        provider="ssga",
        source_url="https://example.test/spy",
    )
    dia_spec = EtfSpec(
        symbol="DIA",
        group="Layer 0",
        issuer="SSGA",
        provider="ssga",
        source_url="https://example.test/dia",
    )
    sessions = [
        FakeSession("validator"),
        FakeSession("worker-1"),
        FakeSession("worker-2"),
    ]
    created_sessions: list[FakeSession] = []
    validator = FakeValidator(valid_symbols={"AAPL", "MSFT"})
    fetch_results = {
        "SPY": FetchResult(
            as_of_date=date(2026, 3, 28),
            source_url=spy_spec.source_url,
            source_format="xlsx",
            rows=[SourceHoldingRow("AAPL", "Apple Inc.", 6.1)],
        ),
        "DIA": FetchResult(
            as_of_date=date(2026, 3, 28),
            source_url=dia_spec.source_url,
            source_format="xlsx",
            rows=[SourceHoldingRow("MSFT", "Microsoft Corp.", 5.0)],
        ),
    }
    active_fetches = 0
    max_active_fetches = 0
    active_lock = threading.Lock()

    def fake_make_session() -> FakeSession:
        if not sessions:
            raise AssertionError("unexpected extra session")
        session = sessions.pop(0)
        created_sessions.append(session)
        return session

    def fake_build_symbol_validator(in_session: FakeSession) -> FakeValidator:
        assert in_session.name == "validator"
        return validator

    def fake_fetch_with_provider(
        in_spec: EtfSpec,
        in_session: FakeSession,
        page: object | None = None,
    ) -> FetchResult:
        nonlocal active_fetches, max_active_fetches
        assert page is None
        assert in_session.name in {"worker-1", "worker-2"}
        with active_lock:
            active_fetches += 1
            max_active_fetches = max(max_active_fetches, active_fetches)
        time.sleep(0.05)
        with active_lock:
            active_fetches -= 1
        return fetch_results[in_spec.symbol]

    def fake_collect_candidate_symbols(in_fetch_result: FetchResult) -> list[str]:
        if in_fetch_result is fetch_results["SPY"]:
            return ["AAPL"]
        if in_fetch_result is fetch_results["DIA"]:
            return ["MSFT"]
        raise AssertionError("unexpected fetch result")

    def fake_normalize_for_storage(
        spec: EtfSpec,
        fetched_at: datetime,
        fetch_result: FetchResult,
        valid_symbols: set[str],
    ) -> tuple[list[NormalizedHoldingRow], HoldingsMeta]:
        assert fetched_at.tzinfo == timezone.utc
        assert valid_symbols == {"AAPL", "MSFT"}
        row = fetch_result.rows[0]
        return (
            [NormalizedHoldingRow(symbol=row.constituent_symbol, name=row.constituent_name, weight=row.weight)],
            HoldingsMeta(
                etfSymbol=spec.symbol,
                issuer=spec.issuer,
                provider=spec.provider,
                asOfDate="2026-03-28",
                fetchedAt="2026-03-31T12:00:00Z",
                sourceUrl=spec.source_url,
                sourceFormat=fetch_result.source_format,
                rowCount=1,
                normalizedRowCount=1,
                droppedRowCount=0,
            ),
        )

    monkeypatch.setattr("etf_universe.cli.make_session", fake_make_session)
    monkeypatch.setattr("etf_universe.cli.build_symbol_validator", fake_build_symbol_validator)
    monkeypatch.setattr("etf_universe.cli.fetch_with_provider", fake_fetch_with_provider)
    monkeypatch.setattr("etf_universe.cli.collect_candidate_symbols", fake_collect_candidate_symbols)
    monkeypatch.setattr("etf_universe.cli.normalize_for_storage", fake_normalize_for_storage)
    monkeypatch.setattr("etf_universe.cli.write_parquet", lambda rows, output_path: None)
    monkeypatch.setattr("etf_universe.cli.write_meta", lambda meta, output_path: None)

    exit_code = main(["fetch", "--symbols", "SPY,DIA", "--output-dir", str(tmp_path)])

    assert exit_code == 0
    assert validator.calls == [["AAPL", "MSFT"]]
    assert max_active_fetches == 2
    assert [session.name for session in created_sessions] == ["validator", "worker-1", "worker-2"]
    assert created_sessions[0].close_calls == 1
    assert created_sessions[1].close_calls == 1
    assert created_sessions[2].close_calls == 1


def test_fetch_preserves_primary_error_when_browser_close_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    session = FakeSession()
    validator = FakeValidator(valid_symbols=set())
    close_calls = 0

    def fake_make_session() -> FakeSession:
        return session

    def fake_build_symbol_validator(in_session: FakeSession) -> FakeValidator:
        assert in_session is session
        return validator

    def fake_launch_browser() -> tuple[object, object, object]:
        return (object(), object(), object())

    def fake_fetch_with_provider(
        in_spec: EtfSpec,
        in_session: FakeSession,
        page: object | None = None,
    ) -> FetchResult:
        assert in_spec.symbol == "QQQ"
        assert in_session is session
        assert page is not None
        raise RuntimeError("primary fetch failure")

    def fake_close_browser(_playwright: object, _browser: object) -> None:
        nonlocal close_calls
        close_calls += 1
        raise RuntimeError("browser close failure")

    monkeypatch.setattr("etf_universe.cli.make_session", fake_make_session)
    monkeypatch.setattr("etf_universe.cli.build_symbol_validator", fake_build_symbol_validator)
    monkeypatch.setattr("etf_universe.cli.launch_browser", fake_launch_browser)
    monkeypatch.setattr("etf_universe.cli.fetch_with_provider", fake_fetch_with_provider)
    monkeypatch.setattr("etf_universe.cli.close_browser", fake_close_browser)

    with pytest.raises(RuntimeError, match="primary fetch failure"):
        main(["fetch", "--symbols", "QQQ", "--output-dir", str(tmp_path)])

    assert close_calls == 1
    assert session.close_calls == 1


def test_fetch_raises_cleanup_error_after_success_and_closes_session(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    session = FakeSession()
    validator = FakeValidator(valid_symbols={"NVDA"})
    close_calls = 0
    qqq_fetch_result = FetchResult(
        as_of_date=date(2026, 3, 28),
        source_url="https://example.test/qqq",
        source_format="json-browser",
        rows=[SourceHoldingRow("NVDA", "NVIDIA Corp.", 6.0)],
    )
    qqq_rows = [NormalizedHoldingRow(symbol="NVDA", name="NVIDIA Corp.", weight=6.0)]
    qqq_meta = HoldingsMeta(
        etfSymbol="QQQ",
        issuer="Invesco",
        provider="invesco",
        asOfDate="2026-03-28",
        fetchedAt="2026-03-31T12:00:00Z",
        sourceUrl="https://example.test/qqq",
        sourceFormat="json-browser",
        rowCount=1,
        normalizedRowCount=1,
        droppedRowCount=0,
    )

    def fake_make_session() -> FakeSession:
        return session

    def fake_build_symbol_validator(in_session: FakeSession) -> FakeValidator:
        assert in_session is session
        return validator

    def fake_launch_browser() -> tuple[object, object, object]:
        return (object(), object(), object())

    def fake_fetch_with_provider(
        in_spec: EtfSpec,
        in_session: FakeSession,
        page: object | None = None,
    ) -> FetchResult:
        assert in_spec.symbol == "QQQ"
        assert in_session is session
        assert page is not None
        return qqq_fetch_result

    def fake_collect_candidate_symbols(in_fetch_result: FetchResult) -> list[str]:
        assert in_fetch_result == qqq_fetch_result
        return ["NVDA"]

    def fake_normalize_for_storage(
        spec: EtfSpec,
        fetched_at: datetime,
        fetch_result: FetchResult,
        valid_symbols: set[str],
    ) -> tuple[list[NormalizedHoldingRow], HoldingsMeta]:
        assert spec.symbol == "QQQ"
        assert fetched_at.tzinfo == timezone.utc
        assert fetch_result == qqq_fetch_result
        assert valid_symbols == {"NVDA"}
        return qqq_rows, qqq_meta

    def fake_write_parquet(rows: list[NormalizedHoldingRow], output_path: Path) -> None:
        assert rows == qqq_rows
        assert output_path == tmp_path / "QQQ.parquet"

    def fake_write_meta(in_meta: HoldingsMeta, output_path: Path) -> None:
        assert in_meta == qqq_meta
        assert output_path == tmp_path / "QQQ.meta.json"

    def fake_close_browser(_playwright: object, _browser: object) -> None:
        nonlocal close_calls
        close_calls += 1
        raise RuntimeError("browser close failure")

    monkeypatch.setattr("etf_universe.cli.make_session", fake_make_session)
    monkeypatch.setattr("etf_universe.cli.build_symbol_validator", fake_build_symbol_validator)
    monkeypatch.setattr("etf_universe.cli.launch_browser", fake_launch_browser)
    monkeypatch.setattr("etf_universe.cli.fetch_with_provider", fake_fetch_with_provider)
    monkeypatch.setattr("etf_universe.cli.collect_candidate_symbols", fake_collect_candidate_symbols)
    monkeypatch.setattr("etf_universe.cli.normalize_for_storage", fake_normalize_for_storage)
    monkeypatch.setattr("etf_universe.cli.write_parquet", fake_write_parquet)
    monkeypatch.setattr("etf_universe.cli.write_meta", fake_write_meta)
    monkeypatch.setattr("etf_universe.cli.close_browser", fake_close_browser)

    with pytest.raises(RuntimeError, match="browser close failure"):
        main(["fetch", "--symbols", "QQQ", "--output-dir", str(tmp_path)])

    assert close_calls == 1
    assert session.close_calls == 1


def test_bare_command_fetches_all_supported_etfs_to_default_output_dir(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spy_spec = EtfSpec(
        symbol="SPY",
        group="Layer 0",
        issuer="SSGA",
        provider="ssga",
        source_url="https://example.test/spy",
    )
    session = FakeSession()
    validator = FakeValidator(valid_symbols={"AAPL"})
    writes: list[tuple[str, Path]] = []
    expected_symbols = list_supported_symbols()

    def fake_make_session() -> FakeSession:
        return session

    def fake_build_symbol_validator(in_session: FakeSession) -> FakeValidator:
        assert in_session is session
        return validator

    def fake_get_specs(symbols: list[str]) -> list[EtfSpec]:
        assert symbols == expected_symbols
        return [spy_spec]

    def fake_fetch_with_provider(
        in_spec: EtfSpec,
        in_session: FakeSession,
        page: object | None = None,
    ) -> FetchResult:
        assert page is None
        assert in_spec == spy_spec
        assert in_session is session
        return FetchResult(
            as_of_date=date(2026, 3, 28),
            source_url=spy_spec.source_url,
            source_format="xlsx",
            rows=[SourceHoldingRow("AAPL", "Apple Inc.", 6.1)],
        )

    def fake_collect_candidate_symbols(in_fetch_result: FetchResult) -> list[str]:
        assert in_fetch_result.rows[0].constituent_symbol == "AAPL"
        return ["AAPL"]

    def fake_normalize_for_storage(
        spec: EtfSpec,
        fetched_at: datetime,
        fetch_result: FetchResult,
        valid_symbols: set[str],
    ) -> tuple[list[NormalizedHoldingRow], HoldingsMeta]:
        assert spec == spy_spec
        assert fetched_at.tzinfo == timezone.utc
        assert fetch_result.rows[0].constituent_symbol == "AAPL"
        assert valid_symbols == {"AAPL"}
        return (
            [NormalizedHoldingRow(symbol="AAPL", name="Apple Inc.", weight=6.1)],
            HoldingsMeta(
                etfSymbol="SPY",
                issuer="SSGA",
                provider="ssga",
                asOfDate="2026-03-28",
                fetchedAt="2026-03-31T12:00:00Z",
                sourceUrl=spy_spec.source_url,
                sourceFormat="xlsx",
                rowCount=1,
                normalizedRowCount=1,
                droppedRowCount=0,
            ),
        )

    def fake_write_parquet(rows: list[NormalizedHoldingRow], output_path: Path) -> None:
        assert rows[0].symbol == "AAPL"
        writes.append(("parquet", output_path))

    def fake_write_meta(in_meta: HoldingsMeta, output_path: Path) -> None:
        assert in_meta.etfSymbol == "SPY"
        writes.append(("meta", output_path))

    monkeypatch.setattr("etf_universe.cli.make_session", fake_make_session)
    monkeypatch.setattr("etf_universe.cli.build_symbol_validator", fake_build_symbol_validator)
    monkeypatch.setattr("etf_universe.cli.get_specs", fake_get_specs)
    monkeypatch.setattr("etf_universe.cli.fetch_with_provider", fake_fetch_with_provider)
    monkeypatch.setattr("etf_universe.cli.collect_candidate_symbols", fake_collect_candidate_symbols)
    monkeypatch.setattr("etf_universe.cli.normalize_for_storage", fake_normalize_for_storage)
    monkeypatch.setattr("etf_universe.cli.write_parquet", fake_write_parquet)
    monkeypatch.setattr("etf_universe.cli.write_meta", fake_write_meta)

    exit_code = main([])

    assert exit_code == 0
    assert validator.calls == [["AAPL"]]
    assert session.close_calls == 1
    assert writes == [
        ("parquet", DEFAULT_OUTPUT_DIR / "SPY.parquet"),
        ("meta", DEFAULT_OUTPUT_DIR / "SPY.meta.json"),
    ]
