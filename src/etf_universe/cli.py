from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

from etf_universe.normalization import collect_candidate_symbols, normalize_for_storage
from etf_universe.providers import close_browser, fetch_with_provider, launch_browser, make_session
from etf_universe.registry import get_specs, list_supported_symbols, parse_symbols_arg
from etf_universe.storage import write_meta, write_parquet
from etf_universe.validation import YFinanceSymbolValidator


DEFAULT_OUTPUT_DIR = Path("data/etf-holdings")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="etf-universe")
    top_level = parser.add_subparsers(dest="topic")

    holdings = top_level.add_parser("holdings")
    holdings_subcommands = holdings.add_subparsers(dest="holdings_command")

    list_supported = holdings_subcommands.add_parser("list-supported")
    list_supported.set_defaults(func=run_holdings_list_supported)

    fetch = holdings_subcommands.add_parser("fetch")
    fetch.add_argument("--symbols", required=True)
    fetch.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    fetch.set_defaults(func=run_holdings_fetch)

    return parser


def run_holdings_list_supported(args: argparse.Namespace) -> int:
    del args
    for symbol in list_supported_symbols():
        print(symbol)
    return 0


def build_symbol_validator(session) -> YFinanceSymbolValidator:  # noqa: ANN001
    del session
    return YFinanceSymbolValidator()


def run_holdings_fetch(args: argparse.Namespace) -> int:
    symbols = parse_symbols_arg(args.symbols)
    specs = get_specs(symbols)
    output_dir = Path(args.output_dir)

    session = make_session()
    validator = build_symbol_validator(session)

    playwright = None
    browser = None
    page = None

    try:
        if any(spec.provider == "invesco" for spec in specs):
            playwright, browser, page = launch_browser()

        fetch_results = []
        candidate_symbols: list[str] = []
        for spec in specs:
            fetch_result = fetch_with_provider(spec, session, page=page)
            fetch_results.append((spec, fetch_result))
            candidate_symbols.extend(collect_candidate_symbols(fetch_result))

        valid_symbols = validator.validate_symbols(candidate_symbols)
        fetched_at = datetime.now(timezone.utc)

        for spec, fetch_result in fetch_results:
            rows, meta = normalize_for_storage(
                spec=spec,
                fetched_at=fetched_at,
                fetch_result=fetch_result,
                valid_symbols=valid_symbols,
            )
            write_parquet(rows, output_dir / f"{spec.symbol}.parquet")
            write_meta(meta, output_dir / f"{spec.symbol}.meta.json")
            print(
                f"{spec.symbol}: kept={meta.normalizedRowCount} dropped={meta.droppedRowCount} "
                f"as_of={meta.asOfDate} provider={spec.issuer}"
            )
    finally:
        if browser is not None and playwright is not None:
            close_browser(playwright, browser)
        session.close()

    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if not hasattr(args, "func"):
        parser.print_help()
        return 1

    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
