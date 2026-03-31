from __future__ import annotations

from collections.abc import Iterable
import time
from typing import Any

import pandas as pd
import yfinance

from etf_universe.normalization import normalize_symbol


YFINANCE_VALIDATION_PERIOD = "5d"
YFINANCE_VALIDATION_INTERVAL = "1d"
YFINANCE_SYMBOL_BATCH_SIZE = 50
YFINANCE_MAX_RETRIES = 3
YFINANCE_RETRY_BACKOFF_SECONDS = 2.0
YFINANCE_REQUIRED_COLUMNS = ("Open", "High", "Low", "Close", "Volume")


def dedupe_symbols(symbols: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for symbol in symbols:
        if symbol in seen:
            continue
        seen.add(symbol)
        deduped.append(symbol)
    return deduped


def chunk_symbols(symbols: list[str], batch_size: int) -> list[list[str]]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    return [symbols[i : i + batch_size] for i in range(0, len(symbols), batch_size)]


def normalize_symbol_for_yahoo(symbol: str) -> str:
    return symbol.replace(".", "-")


def has_usable_ohlcv_rows(data: Any) -> bool:
    if not isinstance(data, pd.DataFrame) or data.empty:
        return False
    if any(column not in data.columns for column in YFINANCE_REQUIRED_COLUMNS):
        return False
    ohlcv = data.loc[:, list(YFINANCE_REQUIRED_COLUMNS)]
    if not (~ohlcv.isna().all(axis=1)).any():
        return False
    close_or_volume = data.loc[:, ["Close", "Volume"]]
    return bool(close_or_volume.notna().any(axis=1).any())


class YFinanceSymbolValidator:
    def __init__(
        self,
        batch_size: int = YFINANCE_SYMBOL_BATCH_SIZE,
        max_retries: int = YFINANCE_MAX_RETRIES,
        retry_backoff_seconds: float = YFINANCE_RETRY_BACKOFF_SECONDS,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if max_retries <= 0:
            raise ValueError("max_retries must be positive")
        if retry_backoff_seconds < 0:
            raise ValueError("retry_backoff_seconds must be non-negative")
        self._batch_size = batch_size
        self._max_retries = max_retries
        self._retry_backoff_seconds = retry_backoff_seconds

    @property
    def enabled(self) -> bool:
        return True

    def validate_symbols(self, symbols: list[str]) -> set[str]:
        normalized_symbols = dedupe_symbols(
            [normalized for symbol in symbols if (normalized := normalize_symbol(symbol)) is not None]
        )
        valid_symbols: set[str] = set()
        for symbol_batch in chunk_symbols(normalized_symbols, self._batch_size):
            valid_symbols.update(self._validate_batch(symbol_batch))
        return valid_symbols

    def _validate_batch(self, symbols: list[str]) -> set[str]:
        if not symbols:
            return set()

        yahoo_to_storage_symbol = {
            normalize_symbol_for_yahoo(symbol): symbol for symbol in symbols
        }
        download_symbols = list(yahoo_to_storage_symbol.keys())
        download_arg: str | Iterable[str] = (
            download_symbols[0] if len(download_symbols) == 1 else download_symbols
        )

        data = self._download_batch(download_arg)

        valid_symbols: set[str] = set()
        if len(download_symbols) == 1:
            yahoo_symbol = download_symbols[0]
            symbol_data = self._extract_symbol_data(data, yahoo_symbol)
            if has_usable_ohlcv_rows(symbol_data):
                storage_symbol = yahoo_to_storage_symbol[download_symbols[0]]
                normalized = normalize_symbol(storage_symbol)
                if normalized is not None:
                    valid_symbols.add(normalized)
            return valid_symbols

        if not isinstance(data, pd.DataFrame) or data.empty:
            return valid_symbols
        if not isinstance(data.columns, pd.MultiIndex):
            return valid_symbols

        available_symbols = set(data.columns.get_level_values(0))
        symbols_to_recheck: list[str] = []
        for yahoo_symbol, storage_symbol in yahoo_to_storage_symbol.items():
            if yahoo_symbol not in available_symbols:
                symbols_to_recheck.append(storage_symbol)
                continue
            if has_usable_ohlcv_rows(data[yahoo_symbol]):
                normalized = normalize_symbol(storage_symbol)
                if normalized is not None:
                    valid_symbols.add(normalized)
                continue
            symbols_to_recheck.append(storage_symbol)

        for storage_symbol in symbols_to_recheck:
            normalized = self._recheck_uncertain_symbol(storage_symbol)
            if normalized is not None:
                valid_symbols.add(normalized)

        return valid_symbols

    def _download_batch(self, download_arg: str | Iterable[str]) -> Any:
        for attempt in range(1, self._max_retries + 1):
            try:
                return yfinance.download(
                    download_arg,
                    period=YFINANCE_VALIDATION_PERIOD,
                    interval=YFINANCE_VALIDATION_INTERVAL,
                    group_by="ticker",
                    auto_adjust=False,
                    progress=False,
                    threads=False,
                )
            except Exception:
                if attempt == self._max_retries:
                    raise
                time.sleep(self._retry_backoff_seconds * attempt)

        raise AssertionError("unreachable")

    def _extract_symbol_data(self, data: Any, yahoo_symbol: str) -> Any:
        if not isinstance(data, pd.DataFrame) or not isinstance(data.columns, pd.MultiIndex):
            return data
        if yahoo_symbol in set(data.columns.get_level_values(0)):
            return data[yahoo_symbol]
        if yahoo_symbol in set(data.columns.get_level_values(-1)):
            return data.xs(yahoo_symbol, axis=1, level=-1)
        return data

    def _recheck_uncertain_symbol(self, storage_symbol: str) -> str | None:
        yahoo_symbol = normalize_symbol_for_yahoo(storage_symbol)
        recheck_attempts = max(1, self._max_retries - 1)
        for attempt in range(1, recheck_attempts + 1):
            data = self._download_batch(yahoo_symbol)
            if has_usable_ohlcv_rows(self._extract_symbol_data(data, yahoo_symbol)):
                normalized = normalize_symbol(storage_symbol)
                if normalized is not None:
                    return normalized
            if attempt != recheck_attempts:
                time.sleep(self._retry_backoff_seconds * attempt)
        return None
