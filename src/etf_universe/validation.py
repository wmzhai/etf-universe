from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
import re
import threading
import time
from typing import Any

import requests

from etf_universe.normalization import normalize_symbol
from etf_universe.runtime_logging import elapsed_ms, log_event


HTTP_TIMEOUT = 60
DEFAULT_ALPACA_DATA_BASE_URL = "https://data.alpaca.markets"
ALPACA_SYMBOL_BATCH_SIZE = 200
ALPACA_MAX_CONCURRENT_BATCHES = 8
INVALID_SYMBOL_MESSAGE_PATTERN = re.compile(
    r"invalid symbol:\s*([A-Z][A-Z0-9.]*)",
    re.IGNORECASE,
)


def _clean_config_value(value: str | None) -> str | None:
    if value is None:
        return None
    text = value.strip()
    return text or None


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


def extract_error_message(response: Any) -> str:
    try:
        payload = response.json()
    except ValueError:
        return str(getattr(response, "text", "")).strip()

    if isinstance(payload, dict) and isinstance(payload.get("message"), str):
        return payload["message"]

    return json.dumps(payload)


def parse_invalid_symbol_from_message(message: str) -> str | None:
    match = INVALID_SYMBOL_MESSAGE_PATTERN.search(message)
    if not match:
        return None
    return normalize_symbol(match.group(1))


class AlpacaDataSymbolValidator:
    def __init__(
        self,
        session: Any,
        api_key: str | None,
        secret_key: str | None,
        base_url: str | None = None,
        batch_size: int = ALPACA_SYMBOL_BATCH_SIZE,
        max_concurrent_batches: int = ALPACA_MAX_CONCURRENT_BATCHES,
        timeout: int = HTTP_TIMEOUT,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if max_concurrent_batches <= 0:
            raise ValueError("max_concurrent_batches must be positive")

        self._session = session
        self._api_key = _clean_config_value(api_key)
        self._secret_key = _clean_config_value(secret_key)
        self._base_url = (_clean_config_value(base_url) or DEFAULT_ALPACA_DATA_BASE_URL).rstrip("/")
        self._batch_size = batch_size
        self._max_concurrent_batches = max_concurrent_batches
        self._timeout = timeout
        self._cache: dict[str, bool] = {}
        self._cache_lock = threading.Lock()

    @property
    def enabled(self) -> bool:
        return bool(self._api_key and self._secret_key)

    def validate_symbols(self, symbols: list[str]) -> set[str]:
        started_at = time.perf_counter()
        normalized_symbols = dedupe_symbols(
            [normalized for symbol in symbols if (normalized := normalize_symbol(symbol)) is not None]
        )
        batches = chunk_symbols(normalized_symbols, self._batch_size)
        log_event(
            "validation.start",
            provider="alpaca",
            input_count=len(symbols),
            normalized_count=len(normalized_symbols),
            batch_count=len(batches),
            concurrent_batches=min(self._max_concurrent_batches, len(batches)) if batches else 0,
        )

        if not self.enabled:
            for symbol in normalized_symbols:
                self._set_cache(symbol, True)
            log_event(
                "validation.disabled",
                provider="alpaca",
                reason="missing_credentials",
            )
            log_event(
                "validation.done",
                provider="alpaca",
                input_count=len(symbols),
                normalized_count=len(normalized_symbols),
                valid_count=len(normalized_symbols),
                elapsed_ms=elapsed_ms(started_at),
            )
            return set(normalized_symbols)

        valid_symbols: set[str] = set()
        if len(batches) <= 1 or self._max_concurrent_batches == 1:
            for batch_index, symbol_batch in enumerate(batches, start=1):
                valid_symbols.update(
                    self._run_batch(
                        symbol_batch,
                        batch_index=batch_index,
                        batch_count=len(batches),
                        session=self._session,
                    )
                )
        else:
            max_workers = min(self._max_concurrent_batches, len(batches))
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [
                    executor.submit(
                        self._run_batch_with_worker_session,
                        symbol_batch,
                        batch_index,
                        len(batches),
                    )
                    for batch_index, symbol_batch in enumerate(batches, start=1)
                ]
                for future in futures:
                    valid_symbols.update(future.result())

        log_event(
            "validation.done",
            provider="alpaca",
            input_count=len(symbols),
            normalized_count=len(normalized_symbols),
            valid_count=len(valid_symbols),
            elapsed_ms=elapsed_ms(started_at),
        )
        return valid_symbols

    def _run_batch_with_worker_session(
        self,
        symbols: list[str],
        batch_index: int,
        batch_count: int,
    ) -> set[str]:
        session = self._make_worker_session()
        try:
            return self._run_batch(
                symbols,
                batch_index=batch_index,
                batch_count=batch_count,
                session=session,
            )
        finally:
            if session is not self._session and hasattr(session, "close"):
                session.close()

    def _make_worker_session(self) -> Any:
        if isinstance(self._session, requests.Session):
            session = requests.Session()
            session.headers.update(self._session.headers)
            return session
        return self._session

    def _run_batch(
        self,
        symbols: list[str],
        *,
        batch_index: int,
        batch_count: int,
        session: Any,
    ) -> set[str]:
        batch_started_at = time.perf_counter()
        log_event(
            "validation.batch.start",
            batch_index=batch_index,
            batch_count=batch_count,
            batch_size=len(symbols),
            symbols=symbols,
        )
        valid_symbols = self._validate_batch(
            symbols,
            batch_index=batch_index,
            batch_count=batch_count,
            session=session,
        )
        log_event(
            "validation.batch.done",
            batch_index=batch_index,
            batch_size=len(symbols),
            valid_count=len(valid_symbols),
            elapsed_ms=elapsed_ms(batch_started_at),
        )
        return valid_symbols

    def _validate_batch(
        self,
        symbols: list[str],
        *,
        batch_index: int,
        batch_count: int,
        session: Any,
    ) -> set[str]:
        remaining = list(symbols)
        endpoint = f"{self._base_url}/v2/stocks/quotes/latest"

        while remaining:
            request_started_at = time.perf_counter()
            log_event(
                "alpaca.request",
                batch_index=batch_index,
                batch_count=batch_count,
                url=endpoint,
                symbol_count=len(remaining),
                symbols=remaining,
            )
            response = session.get(
                endpoint,
                headers={
                    "APCA-API-KEY-ID": self._api_key or "",
                    "APCA-API-SECRET-KEY": self._secret_key or "",
                },
                params={
                    "symbols": ",".join(remaining),
                    "feed": "sip",
                },
                timeout=self._timeout,
            )

            if response.status_code == 200:
                payload = response.json()
                quotes = payload.get("quotes", {}) if isinstance(payload, dict) else {}
                quote_symbols = {
                    normalized
                    for symbol in quotes.keys()
                    if (normalized := normalize_symbol(symbol)) is not None
                }
                valid_symbols = {symbol for symbol in remaining if symbol in quote_symbols}
                invalid_symbols = {symbol for symbol in remaining if symbol not in valid_symbols}

                for symbol in valid_symbols:
                    self._set_cache(symbol, True)
                for symbol in invalid_symbols:
                    self._set_cache(symbol, False)

                log_event(
                    "alpaca.response",
                    batch_index=batch_index,
                    batch_count=batch_count,
                    status=response.status_code,
                    symbol_count=len(remaining),
                    quote_count=len(quote_symbols),
                    valid_count=len(valid_symbols),
                    invalid_count=len(invalid_symbols),
                    elapsed_ms=elapsed_ms(request_started_at),
                )
                return valid_symbols

            log_event(
                "alpaca.response",
                batch_index=batch_index,
                batch_count=batch_count,
                status=response.status_code,
                symbol_count=len(remaining),
                elapsed_ms=elapsed_ms(request_started_at),
            )
            if response.status_code not in {400, 404}:
                response.raise_for_status()

            message = extract_error_message(response)
            invalid_symbol = parse_invalid_symbol_from_message(message)
            if invalid_symbol is None or invalid_symbol not in remaining:
                raise ValueError(f"unable to isolate invalid symbol from Alpaca response: {message}")

            self._set_cache(invalid_symbol, False)
            log_event(
                "alpaca.invalid_symbol",
                batch_index=batch_index,
                batch_count=batch_count,
                symbol=invalid_symbol,
                status=response.status_code,
            )
            remaining = [symbol for symbol in remaining if symbol != invalid_symbol]

        return set()

    def _set_cache(self, symbol: str, value: bool) -> None:
        with self._cache_lock:
            self._cache[symbol] = value
