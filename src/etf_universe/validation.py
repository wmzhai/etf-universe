from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import pandas as pd
import yfinance

from etf_universe.normalization import normalize_symbol


YFINANCE_VALIDATION_PERIOD = "5d"
YFINANCE_VALIDATION_INTERVAL = "1d"
YFINANCE_SYMBOL_BATCH_SIZE = 100
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
    def __init__(self, batch_size: int = YFINANCE_SYMBOL_BATCH_SIZE) -> None:
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        self._batch_size = batch_size

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

        data = yfinance.download(
            download_arg,
            period=YFINANCE_VALIDATION_PERIOD,
            interval=YFINANCE_VALIDATION_INTERVAL,
            group_by="ticker",
            auto_adjust=False,
            progress=False,
            threads=True,
        )

        valid_symbols: set[str] = set()
        if len(download_symbols) == 1:
            if has_usable_ohlcv_rows(data):
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
        for yahoo_symbol, storage_symbol in yahoo_to_storage_symbol.items():
            if yahoo_symbol not in available_symbols:
                continue
            if has_usable_ohlcv_rows(data[yahoo_symbol]):
                normalized = normalize_symbol(storage_symbol)
                if normalized is not None:
                    valid_symbols.add(normalized)

        return valid_symbols
