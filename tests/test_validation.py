import math

import pandas as pd

from etf_universe.validation import YFinanceSymbolValidator, normalize_symbol_for_yahoo


def _make_ohlcv_frame(
    *,
    open_value: float | None,
    high_value: float | None,
    low_value: float | None,
    close_value: float | None,
    volume_value: float | None,
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Open": [open_value],
            "High": [high_value],
            "Low": [low_value],
            "Close": [close_value],
            "Volume": [volume_value],
        }
    )


def test_normalize_symbol_for_yahoo_rewrites_dot_to_dash() -> None:
    assert normalize_symbol_for_yahoo("BRK.B") == "BRK-B"


def test_validate_symbols_keeps_real_ohlcv_and_rejects_all_nan_rows(monkeypatch) -> None:
    calls: list[tuple[object, dict]] = []

    def fake_download(symbols, **kwargs):  # type: ignore[no-untyped-def]
        calls.append((symbols, kwargs))
        assert kwargs["period"] == "5d"
        assert kwargs["interval"] == "1d"
        assert kwargs["group_by"] == "ticker"
        assert kwargs["auto_adjust"] is False
        assert kwargs["progress"] is False
        assert kwargs["threads"] is False
        if symbols == ["BRK-B", "AAPL", "FAKE"]:
            return pd.concat(
                {
                    "BRK-B": _make_ohlcv_frame(
                        open_value=500.0,
                        high_value=505.0,
                        low_value=499.0,
                        close_value=503.0,
                        volume_value=1000.0,
                    ),
                    "AAPL": _make_ohlcv_frame(
                        open_value=200.0,
                        high_value=201.0,
                        low_value=198.0,
                        close_value=199.0,
                        volume_value=5000.0,
                    ),
                    "FAKE": _make_ohlcv_frame(
                        open_value=math.nan,
                        high_value=math.nan,
                        low_value=math.nan,
                        close_value=math.nan,
                        volume_value=math.nan,
                    ),
                },
                axis=1,
            )
        if symbols == "FAKE":
            return _make_ohlcv_frame(
                open_value=math.nan,
                high_value=math.nan,
                low_value=math.nan,
                close_value=math.nan,
                volume_value=math.nan,
            )
        raise AssertionError(f"unexpected symbols: {symbols!r}")

    monkeypatch.setattr("etf_universe.validation.yfinance.download", fake_download)
    validator = YFinanceSymbolValidator()

    valid_symbols = validator.validate_symbols(["brk.b", "AAPL", "FAKE"])

    assert valid_symbols == {"BRK.B", "AAPL"}
    assert len(calls) == 3


def test_validate_symbols_handles_single_symbol_download_shape(monkeypatch) -> None:
    calls: list[tuple[object, dict]] = []

    def fake_download(symbols, **kwargs):  # type: ignore[no-untyped-def]
        calls.append((symbols, kwargs))
        assert symbols == "BRK-B"
        return _make_ohlcv_frame(
            open_value=500.0,
            high_value=505.0,
            low_value=499.0,
            close_value=503.0,
            volume_value=1000.0,
        )

    monkeypatch.setattr("etf_universe.validation.yfinance.download", fake_download)
    validator = YFinanceSymbolValidator()

    valid_symbols = validator.validate_symbols(["brk.b"])

    assert valid_symbols == {"BRK.B"}
    assert len(calls) == 1


def test_validate_symbols_handles_single_symbol_multiindex_download_shape(monkeypatch) -> None:
    calls: list[tuple[object, dict]] = []

    def fake_download(symbols, **kwargs):  # type: ignore[no-untyped-def]
        calls.append((symbols, kwargs))
        assert symbols == "BRK-B"
        return pd.concat(
            {
                "BRK-B": _make_ohlcv_frame(
                    open_value=500.0,
                    high_value=505.0,
                    low_value=499.0,
                    close_value=503.0,
                    volume_value=1000.0,
                )
            },
            axis=1,
        )

    monkeypatch.setattr("etf_universe.validation.yfinance.download", fake_download)
    validator = YFinanceSymbolValidator()

    valid_symbols = validator.validate_symbols(["brk.b"])

    assert valid_symbols == {"BRK.B"}
    assert len(calls) == 1


def test_validate_symbols_splits_large_inputs_into_batches(monkeypatch) -> None:
    call_symbols: list[object] = []

    def fake_download(symbols, **kwargs):  # type: ignore[no-untyped-def]
        call_symbols.append(symbols)
        symbol_list = [symbols] if isinstance(symbols, str) else list(symbols)
        if len(symbol_list) == 1:
            return _make_ohlcv_frame(
                open_value=1.0,
                high_value=1.1,
                low_value=0.9,
                close_value=1.0,
                volume_value=100.0,
            )
        return pd.concat(
            {
                ticker: _make_ohlcv_frame(
                    open_value=1.0,
                    high_value=1.1,
                    low_value=0.9,
                    close_value=1.0,
                    volume_value=100.0,
                )
                for ticker in symbol_list
            },
            axis=1,
        )

    monkeypatch.setattr("etf_universe.validation.yfinance.download", fake_download)
    validator = YFinanceSymbolValidator(batch_size=2)

    input_symbols = ["AAPL", "MSFT", "GOOG", "TSLA", "NVDA"]
    valid_symbols = validator.validate_symbols(input_symbols)

    assert valid_symbols == set(input_symbols)
    assert len(call_symbols) == 3
    assert call_symbols[0] == ["AAPL", "MSFT"]
    assert call_symbols[1] == ["GOOG", "TSLA"]
    assert call_symbols[2] == "NVDA"


def test_validate_symbols_retries_failed_batches_sequentially(monkeypatch) -> None:
    calls: list[tuple[object, dict]] = []
    sleep_calls: list[float] = []

    def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    def fake_download(symbols, **kwargs):  # type: ignore[no-untyped-def]
        calls.append((symbols, kwargs))
        if len(calls) == 1:
            raise OSError("temporary Yahoo failure")
        assert kwargs["threads"] is False
        assert symbols == ["BRK-B", "AAPL"]
        return pd.concat(
            {
                "BRK-B": _make_ohlcv_frame(
                    open_value=500.0,
                    high_value=505.0,
                    low_value=499.0,
                    close_value=503.0,
                    volume_value=1000.0,
                ),
                "AAPL": _make_ohlcv_frame(
                    open_value=200.0,
                    high_value=201.0,
                    low_value=198.0,
                    close_value=199.0,
                    volume_value=5000.0,
                ),
            },
            axis=1,
        )

    monkeypatch.setattr("etf_universe.validation.time.sleep", fake_sleep)
    monkeypatch.setattr("etf_universe.validation.yfinance.download", fake_download)
    validator = YFinanceSymbolValidator(batch_size=2)

    valid_symbols = validator.validate_symbols(["BRK.B", "AAPL"])

    assert valid_symbols == {"BRK.B", "AAPL"}
    assert len(calls) == 2
    assert sleep_calls == [2.0]


def test_validate_symbols_rechecks_symbols_missing_from_partial_batch_results(
    monkeypatch,
) -> None:
    calls: list[object] = []

    def fake_download(symbols, **kwargs):  # type: ignore[no-untyped-def]
        calls.append(symbols)
        assert kwargs["threads"] is False
        if symbols == ["AAPL", "MSFT"]:
            return pd.concat(
                {
                    "AAPL": _make_ohlcv_frame(
                        open_value=200.0,
                        high_value=201.0,
                        low_value=198.0,
                        close_value=199.0,
                        volume_value=5000.0,
                    )
                },
                axis=1,
            )
        if symbols == "MSFT":
            return _make_ohlcv_frame(
                open_value=400.0,
                high_value=401.0,
                low_value=398.0,
                close_value=399.0,
                volume_value=3000.0,
            )
        raise AssertionError(f"unexpected symbols: {symbols!r}")

    monkeypatch.setattr("etf_universe.validation.yfinance.download", fake_download)
    validator = YFinanceSymbolValidator(batch_size=2)

    valid_symbols = validator.validate_symbols(["AAPL", "MSFT"])

    assert valid_symbols == {"AAPL", "MSFT"}
    assert calls == [["AAPL", "MSFT"], "MSFT"]


def test_validate_symbols_rechecks_symbols_with_unusable_batch_slices(
    monkeypatch,
) -> None:
    calls: list[object] = []

    def fake_download(symbols, **kwargs):  # type: ignore[no-untyped-def]
        calls.append(symbols)
        assert kwargs["threads"] is False
        if symbols == ["AAPL", "MSFT"]:
            return pd.concat(
                {
                    "AAPL": _make_ohlcv_frame(
                        open_value=200.0,
                        high_value=201.0,
                        low_value=198.0,
                        close_value=199.0,
                        volume_value=5000.0,
                    ),
                    "MSFT": _make_ohlcv_frame(
                        open_value=math.nan,
                        high_value=math.nan,
                        low_value=math.nan,
                        close_value=math.nan,
                        volume_value=math.nan,
                    ),
                },
                axis=1,
            )
        if symbols == "MSFT":
            return _make_ohlcv_frame(
                open_value=400.0,
                high_value=401.0,
                low_value=398.0,
                close_value=399.0,
                volume_value=3000.0,
            )
        raise AssertionError(f"unexpected symbols: {symbols!r}")

    monkeypatch.setattr("etf_universe.validation.yfinance.download", fake_download)
    validator = YFinanceSymbolValidator(batch_size=2)

    valid_symbols = validator.validate_symbols(["AAPL", "MSFT"])

    assert valid_symbols == {"AAPL", "MSFT"}
    assert calls == [["AAPL", "MSFT"], "MSFT"]


def test_validate_symbols_retries_uncertain_symbol_rechecks(monkeypatch) -> None:
    calls: list[object] = []

    def fake_download(symbols, **kwargs):  # type: ignore[no-untyped-def]
        calls.append(symbols)
        assert kwargs["threads"] is False
        if symbols == ["AAPL", "MSFT"]:
            return pd.concat(
                {
                    "AAPL": _make_ohlcv_frame(
                        open_value=200.0,
                        high_value=201.0,
                        low_value=198.0,
                        close_value=199.0,
                        volume_value=5000.0,
                    ),
                    "MSFT": _make_ohlcv_frame(
                        open_value=math.nan,
                        high_value=math.nan,
                        low_value=math.nan,
                        close_value=math.nan,
                        volume_value=math.nan,
                    ),
                },
                axis=1,
            )
        if symbols == "MSFT" and calls.count("MSFT") == 1:
            return _make_ohlcv_frame(
                open_value=math.nan,
                high_value=math.nan,
                low_value=math.nan,
                close_value=math.nan,
                volume_value=math.nan,
            )
        if symbols == "MSFT":
            return _make_ohlcv_frame(
                open_value=400.0,
                high_value=401.0,
                low_value=398.0,
                close_value=399.0,
                volume_value=3000.0,
            )
        raise AssertionError(f"unexpected symbols: {symbols!r}")

    monkeypatch.setattr("etf_universe.validation.yfinance.download", fake_download)
    validator = YFinanceSymbolValidator(batch_size=2)

    valid_symbols = validator.validate_symbols(["AAPL", "MSFT"])

    assert valid_symbols == {"AAPL", "MSFT"}
    assert calls == [["AAPL", "MSFT"], "MSFT", "MSFT"]
