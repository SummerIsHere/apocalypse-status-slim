from __future__ import annotations

import requests
import pytest

from apocalypse_status.extract.http import HttpClient, HttpConfig


class _FakeResponse:
    def __init__(self, status_code: int = 200, text: str = "ok") -> None:
        self.status_code = status_code
        self.text = text
        self.content = text.encode("utf-8")

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} error", response=self)


def test_http_client_does_not_retry_non_retryable_400(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"count": 0}

    def fake_get(url: str, timeout: int, headers: dict[str, str]) -> _FakeResponse:
        del url, timeout, headers
        calls["count"] += 1
        return _FakeResponse(status_code=400)

    monkeypatch.setattr(requests, "get", fake_get)
    client = HttpClient(HttpConfig(timeout_seconds=1, retries=3, backoff_seconds=0.0))

    try:
        client.get_text("https://example.com")
        assert False, "Expected get_text to raise RuntimeError on 400"
    except RuntimeError:
        pass

    assert calls["count"] == 1


def test_http_client_retries_transient_error(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"count": 0}

    def fake_get(url: str, timeout: int, headers: dict[str, str]) -> _FakeResponse:
        del url, timeout, headers
        calls["count"] += 1
        if calls["count"] == 1:
            raise requests.Timeout("timeout")
        return _FakeResponse(status_code=200, text="ok")

    monkeypatch.setattr(requests, "get", fake_get)
    client = HttpClient(HttpConfig(timeout_seconds=1, retries=3, backoff_seconds=0.0))

    text = client.get_text("https://example.com")

    assert text == "ok"
    assert calls["count"] == 2
