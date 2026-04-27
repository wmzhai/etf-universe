from __future__ import annotations

import re
from dataclasses import fields

from etf_universe.contracts import EtfProfile
from etf_universe.normalization import clean_text, parse_date, parse_float


COMPACT_NUMBER_MULTIPLIERS = {
    "k": 1_000,
    "m": 1_000_000,
    "mm": 1_000_000,
    "million": 1_000_000,
    "b": 1_000_000_000,
    "bn": 1_000_000_000,
    "billion": 1_000_000_000,
    "t": 1_000_000_000_000,
    "trillion": 1_000_000_000_000,
}


def parse_profile_date(value: object) -> str | None:
    try:
        return parse_date(value).isoformat()
    except ValueError:
        return None


def parse_compact_number(value: object) -> float | None:
    text = clean_text(value)
    if text is None:
        return None
    text = text.replace(",", "").replace("$", "").strip()
    if text.startswith("(") and text.endswith(")"):
        text = f"-{text[1:-1]}"

    match = re.fullmatch(r"([-+]?\d+(?:\.\d+)?)\s*([A-Za-z]+)?", text)
    if not match:
        return parse_float(value)

    number = float(match.group(1))
    suffix = (match.group(2) or "").casefold()
    return number * COMPACT_NUMBER_MULTIPLIERS.get(suffix, 1)


def merge_profiles(*profiles: EtfProfile | None) -> EtfProfile:
    values = {}
    for field in fields(EtfProfile):
        values[field.name] = next(
            (
                getattr(profile, field.name)
                for profile in profiles
                if profile is not None and getattr(profile, field.name) is not None
            ),
            None,
        )
    return EtfProfile(**values)


def text_lines(text: str) -> list[str]:
    return [line.strip() for line in text.splitlines() if line.strip()]


def label_value(lines: list[str], label: str) -> str | None:
    target = _normalize_label(label)
    for index, line in enumerate(lines):
        normalized = _normalize_label(line)
        if normalized == target or normalized.startswith(f"{target} "):
            value = _inline_value(line)
            if value is not None:
                return value
            return _next_value(lines, index)
    return None


def label_value_with_as_of(lines: list[str], label: str) -> tuple[str | None, str | None]:
    target = _normalize_label(label)
    for index, line in enumerate(lines):
        normalized = _normalize_label(line)
        if normalized == target or normalized.startswith(f"{target} "):
            value = _inline_value(line)
            if value is not None:
                return value, None

            next_line = _next_value(lines, index)
            if next_line and next_line.casefold().startswith("as of "):
                return _next_value(lines, index + 1), parse_profile_date(next_line)
            return next_line, None
    return None, None


def first_as_of_date(lines: list[str]) -> str | None:
    for line in lines:
        if line.casefold().startswith("as of "):
            parsed = parse_profile_date(line)
            if parsed is not None:
                return parsed
        match = re.search(r"\bas of\s+([^)]+)", line, flags=re.IGNORECASE)
        if match:
            parsed = parse_profile_date(match.group(1))
            if parsed is not None:
                return parsed
    return None


def _normalize_label(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.casefold()).strip()


def _inline_value(line: str) -> str | None:
    if ":" not in line:
        return None
    _, value = line.split(":", 1)
    return clean_text(value)


def _next_value(lines: list[str], index: int) -> str | None:
    for value in lines[index + 1 :]:
        cleaned = clean_text(value)
        if cleaned is not None:
            return cleaned
    return None
