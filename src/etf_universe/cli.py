from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
import time

from etf_universe.normalization import collect_candidate_symbols, normalize_for_storage
from etf_universe.providers import close_browser, fetch_with_provider, launch_browser, make_session
from etf_universe.registry import get_specs, list_supported_symbols, parse_symbols_arg
from etf_universe.runtime_logging import elapsed_ms, log_event
from etf_universe.storage import write_meta, write_parquet
from etf_universe.validation import AlpacaDataSymbolValidator


DEFAULT_OUTPUT_DIR = Path("output")
FETCH_MAX_CONCURRENT_SPECS = 16


def _resolve_output_dir(raw: str | Path) -> Path:
    return Path(raw).expanduser()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="etf-universe")
    parser.set_defaults(func=run_fetch_all)
    parser.add_argument(
        "--output-dir",
        type=_resolve_output_dir,
        default=DEFAULT_OUTPUT_DIR,
    )
    top_level = parser.add_subparsers(dest="command")

    list_command = top_level.add_parser("list")
    list_command.set_defaults(func=run_list_supported)

    fetch = top_level.add_parser("fetch")
    fetch.add_argument("--symbols", required=True)
    fetch.add_argument("--output-dir", type=_resolve_output_dir, default=DEFAULT_OUTPUT_DIR)
    fetch.set_defaults(func=run_fetch)

    return parser


def run_list_supported(args: argparse.Namespace) -> int:
    del args
    for symbol in list_supported_symbols():
        print(symbol)
    return 0


def _read_local_env(path: Path = Path(".env")) -> dict[str, str]:
    if not path.exists():
        return {}

    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def _config_value(name: str, env_values: dict[str, str]) -> str | None:
    value = os.environ.get(name)
    if value is not None and value.strip():
        return value.strip()
    value = env_values.get(name)
    if value is not None and value.strip():
        return value.strip()
    return None


def build_symbol_validator(session) -> AlpacaDataSymbolValidator:  # noqa: ANN001
    env_values = _read_local_env()
    return AlpacaDataSymbolValidator(
        session=session,
        api_key=_config_value("ALPACA_DATA_API_KEY", env_values),
        secret_key=_config_value("ALPACA_DATA_SECRET_KEY", env_values),
        base_url=_config_value("ALPACA_DATA_BASE_URL", env_values),
    )


def _fetch_single_spec(
    spec,
    *,
    session,
    page=None,
):
    provider_started_at = time.perf_counter()
    log_event(
        "provider.fetch.start",
        etf=spec.symbol,
        provider=spec.provider,
        issuer=spec.issuer,
        source_url=spec.source_url,
    )
    fetch_result = fetch_with_provider(spec, session, page=page)
    log_event(
        "provider.fetch.done",
        etf=spec.symbol,
        provider=spec.provider,
        row_count=len(fetch_result.rows),
        as_of=fetch_result.as_of_date.isoformat(),
        source_format=fetch_result.source_format,
        elapsed_ms=elapsed_ms(provider_started_at),
    )
    return spec, fetch_result


def _fetch_single_spec_with_dedicated_session(spec):
    session = make_session()
    primary_error: Exception | None = None
    primary_traceback = None
    cleanup_error: Exception | None = None
    result = None

    try:
        result = _fetch_single_spec(spec, session=session)
    except Exception as exc:
        primary_error = exc
        primary_traceback = exc.__traceback__
    finally:
        try:
            session.close()
        except Exception as exc:
            cleanup_error = exc
            log_event(
                "cleanup.failed",
                resource="fetch_session",
                etf=spec.symbol,
                error_type=type(exc).__name__,
                error=str(exc),
            )

    if primary_error is not None:
        if cleanup_error is not None:
            primary_error.add_note(f"Suppressed cleanup error: {cleanup_error!r}")
        raise primary_error.with_traceback(primary_traceback)
    if cleanup_error is not None:
        raise cleanup_error

    assert result is not None
    return result


def _run_fetch(symbols: list[str], output_dir: Path) -> int:
    total_started_at = time.perf_counter()
    log_event("fetch.start", symbol_count=len(symbols), output_dir=output_dir)
    specs = get_specs(symbols)
    log_event("fetch.specs.resolved", symbols=[spec.symbol for spec in specs])

    setup_started_at = time.perf_counter()
    session = make_session()
    validator = build_symbol_validator(session)

    playwright = None
    browser = None
    page = None
    primary_error: Exception | None = None
    primary_traceback = None
    cleanup_error: Exception | None = None
    valid_symbols: set[str] = set()

    try:
        log_event("phase.done", stage="setup", elapsed_ms=elapsed_ms(setup_started_at))

        fetch_results_by_symbol = {}
        fetch_started_at = time.perf_counter()
        browser_specs = [spec for spec in specs if spec.provider == "invesco"]
        non_browser_specs = [spec for spec in specs if spec.provider != "invesco"]
        non_browser_futures = []

        with ThreadPoolExecutor(
            max_workers=max(1, min(FETCH_MAX_CONCURRENT_SPECS, len(non_browser_specs)))
        ) as executor:
            if len(non_browser_specs) > 1:
                worker_count = min(FETCH_MAX_CONCURRENT_SPECS, len(non_browser_specs))
                log_event(
                    "fetch.parallel.start",
                    worker_count=worker_count,
                    spec_count=len(non_browser_specs),
                )
                non_browser_futures = [
                    executor.submit(_fetch_single_spec_with_dedicated_session, spec)
                    for spec in non_browser_specs
                ]
            else:
                for spec in non_browser_specs:
                    in_spec, fetch_result = _fetch_single_spec(spec, session=session)
                    fetch_results_by_symbol[in_spec.symbol] = fetch_result

            if browser_specs:
                browser_started_at = time.perf_counter()
                log_event("browser.launch.start")
                playwright, browser, page = launch_browser()
                log_event("browser.launch.done", elapsed_ms=elapsed_ms(browser_started_at))
                for spec in browser_specs:
                    in_spec, fetch_result = _fetch_single_spec(spec, session=session, page=page)
                    fetch_results_by_symbol[in_spec.symbol] = fetch_result
                browser_close_started_at = time.perf_counter()
                try:
                    close_browser(playwright, browser)
                    log_event("browser.close.done", elapsed_ms=elapsed_ms(browser_close_started_at))
                finally:
                    playwright = None
                    browser = None
                    page = None

            for future in non_browser_futures:
                in_spec, fetch_result = future.result()
                fetch_results_by_symbol[in_spec.symbol] = fetch_result

        candidate_symbols: list[str] = []
        fetch_results = []
        for spec in specs:
            fetch_result = fetch_results_by_symbol[spec.symbol]
            fetch_results.append((spec, fetch_result))
            candidate_symbols.extend(collect_candidate_symbols(fetch_result))
        log_event(
            "phase.done",
            stage="fetch",
            candidate_count=len(candidate_symbols),
            concurrent_spec_count=min(FETCH_MAX_CONCURRENT_SPECS, len(non_browser_specs)),
            elapsed_ms=elapsed_ms(fetch_started_at),
        )

        validation_started_at = time.perf_counter()
        log_event("validation.start", candidate_count=len(candidate_symbols))
        valid_symbols = validator.validate_symbols(candidate_symbols)
        log_event(
            "validation.done",
            candidate_count=len(candidate_symbols),
            valid_count=len(valid_symbols),
            elapsed_ms=elapsed_ms(validation_started_at),
        )
        fetched_at = datetime.now(timezone.utc)

        write_started_at = time.perf_counter()
        for spec, fetch_result in fetch_results:
            storage_started_at = time.perf_counter()
            rows, meta = normalize_for_storage(
                spec=spec,
                fetched_at=fetched_at,
                fetch_result=fetch_result,
                valid_symbols=valid_symbols,
            )
            parquet_path = output_dir / f"{spec.symbol}.parquet"
            meta_path = output_dir / f"{spec.symbol}.meta.json"
            write_parquet(rows, parquet_path)
            write_meta(meta, meta_path)
            log_event(
                "storage.write.done",
                etf=spec.symbol,
                normalized_row_count=meta.normalizedRowCount,
                dropped_row_count=meta.droppedRowCount,
                parquet_path=parquet_path,
                meta_path=meta_path,
                elapsed_ms=elapsed_ms(storage_started_at),
            )
            print(
                f"{spec.symbol}: kept={meta.normalizedRowCount} dropped={meta.droppedRowCount} "
                f"as_of={meta.asOfDate} provider={spec.issuer}"
            )
        log_event("phase.done", stage="write", etf_count=len(fetch_results), elapsed_ms=elapsed_ms(write_started_at))
    except Exception as exc:
        primary_error = exc
        primary_traceback = exc.__traceback__
        log_event(
            "fetch.failed",
            error_type=type(exc).__name__,
            error=str(exc),
            elapsed_ms=elapsed_ms(total_started_at),
        )
    finally:
        cleanup_started_at = time.perf_counter()
        if browser is not None and playwright is not None:
            try:
                close_browser(playwright, browser)
            except Exception as exc:
                if cleanup_error is None:
                    cleanup_error = exc
                log_event(
                    "cleanup.failed",
                    resource="browser",
                    error_type=type(exc).__name__,
                    error=str(exc),
                )
        try:
            session.close()
        except Exception as exc:
            if cleanup_error is None:
                cleanup_error = exc
            log_event(
                "cleanup.failed",
                resource="session",
                error_type=type(exc).__name__,
                error=str(exc),
            )
        log_event("phase.done", stage="cleanup", elapsed_ms=elapsed_ms(cleanup_started_at))

    if primary_error is not None:
        if cleanup_error is not None:
            primary_error.add_note(f"Suppressed cleanup error: {cleanup_error!r}")
        raise primary_error.with_traceback(primary_traceback)
    if cleanup_error is not None:
        raise cleanup_error

    log_event(
        "fetch.done",
        etf_count=len(specs),
        valid_symbol_count=len(valid_symbols),
        output_dir=output_dir,
        elapsed_ms=elapsed_ms(total_started_at),
    )

    return 0


def run_fetch(args: argparse.Namespace) -> int:
    return _run_fetch(parse_symbols_arg(args.symbols), _resolve_output_dir(args.output_dir))


def run_fetch_all(args: argparse.Namespace) -> int:
    return _run_fetch(list_supported_symbols(), _resolve_output_dir(args.output_dir))


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    if argv is None:
        argv = sys.argv[1:]
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
