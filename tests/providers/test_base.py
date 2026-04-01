from __future__ import annotations

from typing import Any

from etf_universe.providers.base import request_with_logging


class FakeResponse:
    def __init__(self, status_code: int, content: bytes) -> None:
        self.status_code = status_code
        self.content = content


class FakeSession:
    def __init__(self, response: FakeResponse) -> None:
        self.response = response
        self.calls: list[dict[str, Any]] = []

    def request(self, method: str, url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append({"method": method, "url": url, **kwargs})
        return self.response


def test_request_with_logging_logs_request_and_response(capsys) -> None:
    response = FakeResponse(status_code=200, content=b"abc")
    session = FakeSession(response)

    result = request_with_logging(session, "GET", "https://example.test/data", timeout=10)

    assert result is response
    assert session.calls == [
        {"method": "GET", "url": "https://example.test/data", "timeout": 10}
    ]

    captured = capsys.readouterr()
    assert "event=http.request" in captured.err
    assert "method=GET" in captured.err
    assert "url=https://example.test/data" in captured.err
    assert "event=http.response" in captured.err
    assert "status=200" in captured.err
    assert "bytes=3" in captured.err
