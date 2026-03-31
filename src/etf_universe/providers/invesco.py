from __future__ import annotations

import json
from typing import Any

from playwright.sync_api import Browser, Page, sync_playwright

from etf_universe.contracts import EtfSpec, FetchResult, SourceHoldingRow
from etf_universe.normalization import clean_text, parse_date
from etf_universe.providers.base import build_source_row


def browser_fetch_json(page: Page, api_url: str) -> dict[str, Any]:
    result = page.evaluate(
        """async (url) => {
            const response = await fetch(url.replace(/&amp;/g, '&'));
            return { status: response.status, text: await response.text() };
        }""",
        api_url,
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


def fetch_invesco(spec: EtfSpec, page: Page) -> FetchResult:
    page.goto(spec.source_url, wait_until="domcontentloaded", timeout=120000)

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
    payload = browser_fetch_json(page, api_url)
    return parse_invesco_payload(payload, source_url)


def launch_browser() -> tuple[Any, Browser, Page]:
    playwright = sync_playwright().start()
    try:
        try:
            browser = playwright.chromium.launch(channel="chrome", headless=True)
        except Exception:
            browser = playwright.chromium.launch(headless=True)
    except Exception:
        playwright.stop()
        raise
    return playwright, browser, browser.new_page()


def close_browser(playwright: Any, browser: Browser) -> None:
    browser.close()
    playwright.stop()
