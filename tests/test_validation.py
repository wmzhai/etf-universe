from __future__ import annotations

from collections.abc import Callable
import json
import threading
import time

import pytest
import requests

from etf_universe.validation import (
    ALPACA_MAX_CONCURRENT_BATCHES,
    ALPACA_SYMBOL_BATCH_SIZE,
    AlpacaDataSymbolValidator,
    parse_invalid_symbol_from_message,
)


class FakeResponse:
    def __init__(self, status_code: int, payload: object) -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = json.dumps(payload)

    def json(self) -> object:
        return self._payload

    def raise_for_status(self) -> None:
        raise requests.HTTPError(f"HTTP {self.status_code}")


class FakeSession:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self._responses = responses
        self.calls: list[dict[str, object]] = []

    def get(self, url: str, **kwargs: object) -> FakeResponse:
        self.calls.append({"url": url, **kwargs})
        if not self._responses:
            raise AssertionError("unexpected extra GET call")
        return self._responses.pop(0)


def test_parse_invalid_symbol_from_message_extracts_dot_form_symbol() -> None:
    assert (
        parse_invalid_symbol_from_message("code=400, message=invalid symbol: BRK.B")
        == "BRK.B"
    )


def test_validate_symbols_keeps_only_symbols_present_in_quotes_payload(capsys) -> None:
    session = FakeSession(
        [
            FakeResponse(
                200,
                {
                    "quotes": {
                        "AAPL": {"bp": 1},
                        "NVDA": {"bp": 2},
                    }
                },
            )
        ]
    )
    validator = AlpacaDataSymbolValidator(
        session=session,
        api_key="key",
        secret_key="secret",
        base_url="https://data.alpaca.markets",
        max_concurrent_batches=1,
    )

    valid_symbols = validator.validate_symbols(["AAPL", "HEIA", "NVDA"])

    assert valid_symbols == {"AAPL", "NVDA"}
    assert validator._cache["AAPL"] is True
    assert validator._cache["NVDA"] is True
    assert validator._cache["HEIA"] is False
    assert session.calls == [
        {
            "url": "https://data.alpaca.markets/v2/stocks/quotes/latest",
            "headers": {
                "APCA-API-KEY-ID": "key",
                "APCA-API-SECRET-KEY": "secret",
            },
            "params": {
                "symbols": "AAPL,HEIA,NVDA",
                "feed": "sip",
            },
            "timeout": 60,
        }
    ]
    captured = capsys.readouterr()
    assert "event=validation.start" in captured.err
    assert "event=alpaca.request" in captured.err
    assert "event=alpaca.response" in captured.err
    assert "status=200" in captured.err
    assert "quote_count=2" in captured.err
    assert "event=validation.done" in captured.err
    assert "valid_count=2" in captured.err


def test_validate_symbols_removes_invalid_symbol_and_retries_remaining_batch(capsys) -> None:
    session = FakeSession(
        [
            FakeResponse(400, {"message": "code=400, message=invalid symbol: IXAM6"}),
            FakeResponse(
                200,
                {
                    "quotes": {
                        "AAPL": {"bp": 1},
                        "NVDA": {"bp": 2},
                    }
                },
            ),
        ]
    )
    validator = AlpacaDataSymbolValidator(
        session=session,
        api_key="key",
        secret_key="secret",
        base_url="https://data.alpaca.markets",
        max_concurrent_batches=1,
    )

    valid_symbols = validator.validate_symbols(["AAPL", "IXAM6", "NVDA"])

    assert valid_symbols == {"AAPL", "NVDA"}
    assert validator._cache["IXAM6"] is False
    assert len(session.calls) == 2
    assert session.calls[0]["params"] == {"symbols": "AAPL,IXAM6,NVDA", "feed": "sip"}
    assert session.calls[1]["params"] == {"symbols": "AAPL,NVDA", "feed": "sip"}
    captured = capsys.readouterr()
    assert "event=alpaca.invalid_symbol" in captured.err
    assert "symbol=IXAM6" in captured.err


def test_validate_symbols_keeps_dot_form_share_class_for_alpaca() -> None:
    session = FakeSession(
        [
            FakeResponse(
                200,
                {
                    "quotes": {
                        "BRK.B": {"bp": 1},
                    }
                },
            )
        ]
    )
    validator = AlpacaDataSymbolValidator(
        session=session,
        api_key="key",
        secret_key="secret",
        base_url="https://data.alpaca.markets",
        max_concurrent_batches=1,
    )

    valid_symbols = validator.validate_symbols(["brk/b"])

    assert valid_symbols == {"BRK.B"}
    assert session.calls[0]["params"] == {"symbols": "BRK.B", "feed": "sip"}


def test_validate_symbols_splits_large_inputs_into_batches() -> None:
    session = FakeSession(
        [
            FakeResponse(200, {"quotes": {"AAPL": {}, "MSFT": {}}}),
            FakeResponse(200, {"quotes": {"GOOG": {}, "TSLA": {}}}),
            FakeResponse(200, {"quotes": {"NVDA": {}}}),
        ]
    )
    validator = AlpacaDataSymbolValidator(
        session=session,
        api_key="key",
        secret_key="secret",
        base_url="https://data.alpaca.markets",
        batch_size=2,
        max_concurrent_batches=1,
    )

    valid_symbols = validator.validate_symbols(["AAPL", "MSFT", "GOOG", "TSLA", "NVDA"])

    assert valid_symbols == {"AAPL", "MSFT", "GOOG", "TSLA", "NVDA"}
    assert [call["params"] for call in session.calls] == [
        {"symbols": "AAPL,MSFT", "feed": "sip"},
        {"symbols": "GOOG,TSLA", "feed": "sip"},
        {"symbols": "NVDA", "feed": "sip"},
    ]


def test_validate_symbols_runs_batches_concurrently(monkeypatch: pytest.MonkeyPatch) -> None:
    validator = AlpacaDataSymbolValidator(
        session=FakeSession([]),
        api_key="key",
        secret_key="secret",
        base_url="https://data.alpaca.markets",
        batch_size=2,
        max_concurrent_batches=2,
    )
    active_batches = 0
    max_active_batches = 0
    active_lock = threading.Lock()

    def fake_validate_batch(
        symbols: list[str],
        *,
        batch_index: int,
        batch_count: int,
        session: object,
    ) -> set[str]:
        nonlocal active_batches, max_active_batches
        assert session is not None
        assert batch_count == 2
        with active_lock:
            active_batches += 1
            max_active_batches = max(max_active_batches, active_batches)
        time.sleep(0.05)
        with active_lock:
            active_batches -= 1
        return set(symbols)

    monkeypatch.setattr(validator, "_validate_batch", fake_validate_batch)
    monkeypatch.setattr(
        validator,
        "_make_worker_session",
        lambda: object(),
    )

    valid_symbols = validator.validate_symbols(["AAPL", "MSFT", "GOOG", "TSLA"])

    assert valid_symbols == {"AAPL", "MSFT", "GOOG", "TSLA"}
    assert max_active_batches == 2


def test_validate_symbols_returns_all_symbols_when_credentials_missing() -> None:
    session = FakeSession([])
    validator = AlpacaDataSymbolValidator(
        session=session,
        api_key=None,
        secret_key=None,
        base_url="https://data.alpaca.markets",
    )

    valid_symbols = validator.validate_symbols(["AAPL", "BRK.B", "AAPL"])

    assert valid_symbols == {"AAPL", "BRK.B"}
    assert session.calls == []


def test_default_alpaca_validation_settings_match_benchmarked_defaults() -> None:
    assert ALPACA_SYMBOL_BATCH_SIZE == 200
    assert ALPACA_MAX_CONCURRENT_BATCHES == 8
