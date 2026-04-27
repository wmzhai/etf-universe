from __future__ import annotations

from typing import Any

import pytest

from etf_universe.contracts import EtfSpec
from etf_universe.providers.invesco import (
    browser_fetch_json,
    build_rsp_api_url,
    close_browser,
    fetch_invesco,
    launch_browser,
    parse_invesco_payload,
    parse_invesco_profile_text,
)


class FakeEvaluatePage:
    def __init__(self, result: dict[str, Any]) -> None:
        self.result = result
        self.calls: list[str] = []

    def evaluate(self, _script: str, api_url: str) -> dict[str, Any]:
        self.calls.append(api_url)
        return self.result


class FakeLocator:
    def __init__(self, attrs: dict[str, str | None]) -> None:
        self.attrs = attrs
        self.wait_calls: list[dict[str, Any]] = []

    def wait_for(self, **kwargs: Any) -> None:
        self.wait_calls.append(kwargs)

    def get_attribute(self, name: str) -> str | None:
        return self.attrs.get(name)


class FakeLocatorQuery:
    def __init__(self, locator: FakeLocator) -> None:
        self.first = locator


class FakeInvescoPage:
    def __init__(self, by_selector: dict[str, FakeLocator]) -> None:
        self.by_selector = by_selector
        self.goto_calls: list[dict[str, Any]] = []

    def goto(self, url: str, **kwargs: Any) -> None:
        self.goto_calls.append({"url": url, **kwargs})

    def locator(self, selector: str) -> FakeLocatorQuery:
        return FakeLocatorQuery(self.by_selector[selector])


def make_spec(symbol: str) -> EtfSpec:
    return EtfSpec(
        symbol=symbol,
        group="core",
        issuer="Invesco",
        provider="invesco",
        source_url=f"https://example.test/{symbol.lower()}",
    )


def test_build_rsp_api_url_uses_isin_to_derive_cusip() -> None:
    assert build_rsp_api_url("US46137V3574").endswith(
        "/46137V357/holdings/fund?idType=cusip&productType=ETF"
    )


def test_parse_invesco_payload_extracts_holdings_rows() -> None:
    payload = {
        "effectiveDate": "2026-03-28",
        "holdings": [
            {
                "ticker": "AAPL",
                "issuerName": "Apple Inc.",
                "percentageOfTotalNetAssets": "6.1",
                "assetClassName": "Equity",
                "securityTypeName": "Common Stock",
            }
        ],
    }

    result = parse_invesco_payload(payload, "https://api.example.test/qqq")

    assert result.as_of_date.isoformat() == "2026-03-28"
    assert result.source_format == "json-browser"
    assert result.rows[0].constituent_symbol == "AAPL"


def test_parse_invesco_profile_text_extracts_product_details() -> None:
    profile = parse_invesco_profile_text(
        title="Invesco QQQ ETF | Invesco US",
        body_text="""as of 4/25/2026
Fund ticker
QQQ
Exchange
Nasdaq/NMS (Global Market)
Inception date
03/09/1999
Total Expense Ratio
0.18%
Assets Under Management
$435.32B
Shares Outstanding
655.45M
""",
        source_url="https://example.com/qqq",
    )

    assert profile.fundName == "Invesco QQQ ETF"
    assert profile.exchange == "Nasdaq/NMS (Global Market)"
    assert profile.inceptionDate == "1999-03-09"
    assert profile.expenseRatio == 0.18
    assert profile.assetsUnderManagement == 435320000000.0
    assert profile.sharesOutstanding == 655450000.0
    assert profile.profileAsOfDate == "2026-04-25"


def test_parse_invesco_profile_text_ignores_generic_holdings_title() -> None:
    profile = parse_invesco_profile_text(
        title="Holdings & Sector Allocations of Invesco QQQ | Invesco US",
        body_text="",
        source_url="https://example.com/qqq",
    )

    assert profile.fundName is None


def test_browser_fetch_json_parses_json_text(capsys) -> None:
    page = FakeEvaluatePage({"status": 200, "text": '{"hello":"world"}'})
    assert browser_fetch_json(page, "https://api.example.test/data") == {"hello": "world"}
    assert page.calls == ["https://api.example.test/data"]
    captured = capsys.readouterr()
    assert "event=browser.request" in captured.err
    assert "url=https://api.example.test/data" in captured.err
    assert "event=browser.response" in captured.err
    assert "status=200" in captured.err


def test_browser_fetch_json_raises_on_non_200() -> None:
    page = FakeEvaluatePage({"status": 503, "text": '{"error":"unavailable"}'})
    with pytest.raises(ValueError, match="Browser fetch failed: 503"):
        browser_fetch_json(page, "https://api.example.test/data")


def test_fetch_invesco_qqq_uses_data_holding_api(monkeypatch: pytest.MonkeyPatch) -> None:
    locator = FakeLocator(
        {"data-holding-api": "https://api.example.test/qqq?x=1&amp;y=2"}
    )
    page = FakeInvescoPage({"[data-holding-api]": locator})
    payload = {"effectiveDate": "2026-03-28", "holdings": []}
    captured: dict[str, str] = {}

    def fake_browser_fetch_json(_page: Any, api_url: str) -> dict[str, Any]:
        captured["api_url"] = api_url
        return payload

    monkeypatch.setattr("etf_universe.providers.invesco.browser_fetch_json", fake_browser_fetch_json)
    result = fetch_invesco(make_spec("QQQ"), page)

    assert captured["api_url"] == "https://api.example.test/qqq?x=1&amp;y=2"
    assert result.source_url == "https://api.example.test/qqq?x=1&y=2"
    assert locator.wait_calls == [{"state": "attached", "timeout": 60000}]


def test_fetch_invesco_rsp_derives_api_url_from_isin(monkeypatch: pytest.MonkeyPatch) -> None:
    locator = FakeLocator({"content": " US46137V3574 "})
    page = FakeInvescoPage({'meta[name="isin"]': locator})
    payload = {"effectiveDate": "2026-03-28", "holdings": []}
    captured: dict[str, str] = {}

    def fake_browser_fetch_json(_page: Any, api_url: str) -> dict[str, Any]:
        captured["api_url"] = api_url
        return payload

    monkeypatch.setattr("etf_universe.providers.invesco.browser_fetch_json", fake_browser_fetch_json)
    result = fetch_invesco(make_spec("RSP"), page)

    expected = build_rsp_api_url("US46137V3574")
    assert captured["api_url"] == expected
    assert result.source_url == expected
    assert locator.wait_calls == [{"state": "attached", "timeout": 60000}]


def test_fetch_invesco_unsupported_symbol_raises_value_error() -> None:
    page = FakeInvescoPage({})
    with pytest.raises(ValueError, match="Unsupported Invesco symbol: IVV"):
        fetch_invesco(make_spec("IVV"), page)


def test_launch_browser_cleans_up_when_new_page_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: dict[str, int] = {"stop": 0, "close": 0}

    class FakeBrowser:
        def new_page(self) -> Any:
            raise RuntimeError("new_page failed")

        def close(self) -> None:
            calls["close"] += 1

    class FakeChromium:
        def launch(self, **_kwargs: Any) -> FakeBrowser:
            return FakeBrowser()

    class FakePlaywright:
        chromium = FakeChromium()

        def stop(self) -> None:
            calls["stop"] += 1

    class FakeManager:
        def start(self) -> FakePlaywright:
            return FakePlaywright()

    monkeypatch.setattr("etf_universe.providers.invesco.sync_playwright", lambda: FakeManager())

    with pytest.raises(RuntimeError, match="new_page failed"):
        launch_browser()

    assert calls["close"] == 1
    assert calls["stop"] == 1


def test_close_browser_stops_playwright_when_browser_close_raises() -> None:
    calls: dict[str, int] = {"stop": 0}

    class BrokenBrowser:
        def close(self) -> None:
            raise RuntimeError("close failed")

    class FakePlaywright:
        def stop(self) -> None:
            calls["stop"] += 1

    with pytest.raises(RuntimeError, match="close failed"):
        close_browser(FakePlaywright(), BrokenBrowser())

    assert calls["stop"] == 1
