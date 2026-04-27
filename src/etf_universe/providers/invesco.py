from __future__ import annotations

import json
import time
from typing import Any

from playwright.sync_api import Browser, Page, sync_playwright

from etf_universe.contracts import EtfProfile, EtfSpec, FetchResult, SourceHoldingRow
from etf_universe.normalization import clean_text, parse_date
from etf_universe.profile import (
    first_as_of_date,
    label_value,
    merge_profiles,
    parse_compact_number,
    parse_profile_date,
    text_lines,
)
from etf_universe.providers.base import build_source_row
from etf_universe.runtime_logging import elapsed_ms, log_event


INVESCO_FUND_NAMES = {
    "QQQ": "Invesco QQQ ETF",
    "RSP": "Invesco S&P 500 Equal Weight ETF",
}


def browser_fetch_json(page: Page, api_url: str) -> dict[str, Any]:
    log_event("browser.request", url=api_url)
    started_at = time.perf_counter()
    result = page.evaluate(
        """async (url) => {
            const response = await fetch(url.replace(/&amp;/g, '&'));
            return { status: response.status, text: await response.text() };
        }""",
        api_url,
    )
    log_event(
        "browser.response",
        url=api_url.replace("&amp;", "&"),
        status=result["status"],
        bytes=len(result["text"].encode("utf-8")),
        elapsed_ms=elapsed_ms(started_at),
    )
    if result["status"] != 200:
        raise ValueError(f"Browser fetch failed: {result['status']} {api_url}")
    return json.loads(result["text"])


def build_rsp_api_url(isin: str) -> str:
    cleaned = isin.strip()
    if len(cleaned) < 11:
        raise ValueError("Unable to derive CUSIP from RSP ISIN")
    cusip = cleaned[2:11]
    return (
        "https://dng-api.invesco.com/cache/v1/accounts/en_US/shareclasses/"
        f"{cusip}/holdings/fund?idType=cusip&productType=ETF"
    )


def parse_invesco_payload(payload: dict[str, Any], source_url: str) -> FetchResult:
    as_of_date = parse_date(payload["effectiveDate"])
    records: list[SourceHoldingRow] = []

    for row in payload["holdings"]:
        symbol = row.get("ticker")
        name = row.get("issuerName")
        if clean_text(symbol) is None and clean_text(name) is None:
            continue
        records.append(
            build_source_row(
                constituent_symbol=symbol,
                constituent_name=name,
                weight=row.get("percentageOfTotalNetAssets"),
                asset_class=row.get("assetClassName") or row.get("assetClass"),
                security_type=row.get("securityTypeName") or row.get("securityTypeCode"),
            )
        )

    return FetchResult(
        as_of_date=as_of_date,
        source_url=source_url,
        source_format="json-browser",
        rows=records,
    )


def parse_invesco_profile_text(title: str | None, body_text: str, source_url: str) -> EtfProfile:
    lines = text_lines(body_text)
    fund_name = _fund_name_from_title(title)
    assets_under_management = (
        label_value(lines, "Assets Under Management")
        or label_value(lines, "Market value")
    )

    return EtfProfile(
        fundName=fund_name,
        exchange=label_value(lines, "Exchange"),
        cusip=label_value(lines, "CUSIP"),
        isin=label_value(lines, "ISIN"),
        inceptionDate=parse_profile_date(label_value(lines, "Inception date")),
        expenseRatio=parse_compact_number(label_value(lines, "Total Expense Ratio")),
        netExpenseRatio=parse_compact_number(label_value(lines, "Net expense ratio")),
        assetsUnderManagement=parse_compact_number(assets_under_management),
        sharesOutstanding=parse_compact_number(label_value(lines, "Shares Outstanding")),
        profileAsOfDate=first_as_of_date(lines),
        profileSourceUrl=source_url,
    )


def _fund_name_from_title(title: str | None) -> str | None:
    if title is None:
        return None
    name = clean_text(title.split("|", 1)[0])
    if name is None:
        return None
    normalized = " ".join(name.casefold().replace("&", "and").split())
    if normalized.startswith("holdings and sector allocations"):
        return None
    return name


def fetch_invesco(spec: EtfSpec, page: Page) -> FetchResult:
    log_event("browser.goto.start", etf=spec.symbol, url=spec.source_url)
    started_at = time.perf_counter()
    page_response = page.goto(spec.source_url, wait_until="domcontentloaded", timeout=120000)
    try:
        page.wait_for_load_state("networkidle", timeout=30000)
    except Exception:
        pass
    page_status = getattr(page_response, "status", None)
    log_event(
        "browser.goto.done",
        etf=spec.symbol,
        url=spec.source_url,
        status=page_status if page_status is not None else "unknown",
        elapsed_ms=elapsed_ms(started_at),
    )

    if spec.symbol == "QQQ":
        locator = page.locator("[data-holding-api]").first
        locator.wait_for(state="attached", timeout=60000)
        api_url = locator.get_attribute("data-holding-api")
        if not api_url:
            raise ValueError("Unable to find QQQ data-holding-api")
    elif spec.symbol == "RSP":
        locator = page.locator('meta[name="isin"]').first
        locator.wait_for(state="attached", timeout=60000)
        isin = locator.get_attribute("content")
        if not isin:
            raise ValueError("Unable to find RSP ISIN")
        api_url = build_rsp_api_url(isin)
    else:
        raise ValueError(f"Unsupported Invesco symbol: {spec.symbol}")

    source_url = api_url.replace("&amp;", "&")
    try:
        profile = parse_invesco_profile_text(
            title=page.title(),
            body_text=page.locator("body").inner_text(timeout=10000),
            source_url=spec.source_url,
        )
    except Exception:
        profile = EtfProfile(profileSourceUrl=spec.source_url)
    profile = merge_profiles(
        EtfProfile(fundName=INVESCO_FUND_NAMES.get(spec.symbol), profileSourceUrl=spec.source_url),
        profile,
    )

    payload = browser_fetch_json(page, api_url)
    result = parse_invesco_payload(payload, source_url)
    return FetchResult(
        as_of_date=result.as_of_date,
        source_url=result.source_url,
        source_format=result.source_format,
        rows=result.rows,
        profile=merge_profiles(profile, result.profile),
    )


def launch_browser() -> tuple[Any, Browser, Page]:
    playwright = sync_playwright().start()
    browser: Browser | None = None
    try:
        try:
            browser = playwright.chromium.launch(channel="chrome", headless=True)
        except Exception:
            browser = playwright.chromium.launch(headless=True)
        page = browser.new_page()
        return playwright, browser, page
    except Exception:
        if browser is not None:
            try:
                browser.close()
            except Exception:
                pass
        try:
            playwright.stop()
        except Exception:
            pass
        raise


def close_browser(playwright: Any, browser: Browser) -> None:
    close_error: Exception | None = None
    try:
        browser.close()
    except Exception as exc:
        close_error = exc

    try:
        playwright.stop()
    except Exception:
        if close_error is None:
            raise

    if close_error is not None:
        raise close_error
