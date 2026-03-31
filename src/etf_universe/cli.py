from __future__ import annotations

import argparse

from etf_universe.registry import list_supported_symbols


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="etf-universe")
    top_level = parser.add_subparsers(dest="topic")

    holdings = top_level.add_parser("holdings")
    holdings_subcommands = holdings.add_subparsers(dest="holdings_command")

    list_supported = holdings_subcommands.add_parser("list-supported")
    list_supported.set_defaults(func=run_holdings_list_supported)

    return parser


def run_holdings_list_supported(args: argparse.Namespace) -> int:
    del args
    for symbol in list_supported_symbols():
        print(symbol)
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
