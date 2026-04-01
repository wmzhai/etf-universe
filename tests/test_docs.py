from pathlib import Path
import re


DOC_PATHS = [
    Path("AGENTS.md"),
    Path("README.md"),
    Path("docs/superpowers/specs/2026-03-31-etf-universe-design.md"),
    Path("docs/superpowers/plans/2026-03-31-etf-universe.md"),
]

LEGACY_PATTERNS = {
    r"uv run etf-universe holdings\b": "Deprecated nested CLI must not appear in docs.",
    r"\bholdings list-supported\b": "Deprecated nested CLI must not appear in docs.",
    r"\bholdings fetch\b": "Deprecated nested CLI must not appear in docs.",
    r"\blist-supported\b": "Deprecated list-supported command must not appear in docs.",
    r"\bdata/etf-holdings\b": "Deprecated output directory must not appear in docs.",
    r"\byfinance\b": "Removed validation backend must not appear in docs.",
    r"\bOHLCV\b": "Removed yfinance validation rules must not appear in docs.",
    r"remove the Alpaca credential dependency": "Docs must describe the current Alpaca-based flow.",
    r"does not require API credentials": "Docs must not contradict the current optional Alpaca credential configuration.",
}


def test_markdown_docs_do_not_reference_removed_cli_or_validation_flows() -> None:
    for path in DOC_PATHS:
        content = path.read_text(encoding="utf-8")
        for pattern, reason in LEGACY_PATTERNS.items():
            assert re.search(pattern, content) is None, f"{path} matches /{pattern}/. {reason}"
