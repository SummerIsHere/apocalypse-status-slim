"""HTTP client wrapper with retry/backoff."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

import requests


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class HttpConfig:
    timeout_seconds: int = 45
    retries: int = 3
    backoff_seconds: float = 1.5
    user_agent: str = "apocalypse-status/0.1 (+local-pipeline)"


class HttpClient:
    def __init__(self, config: HttpConfig | None = None) -> None:
        self.config = config or HttpConfig()

    def get_text(self, url: str, *, suppress_non_retryable_log: bool = False) -> str:
        response = self._request(url, suppress_non_retryable_log=suppress_non_retryable_log)
        return response.text

    def get_bytes(self, url: str, *, suppress_non_retryable_log: bool = False) -> bytes:
        response = self._request(url, suppress_non_retryable_log=suppress_non_retryable_log)
        return response.content

    def _request(self, url: str, *, suppress_non_retryable_log: bool = False) -> requests.Response:
        last_exc: Exception | None = None
        headers = {"User-Agent": self.config.user_agent}
        for attempt in range(1, self.config.retries + 1):
            try:
                response = requests.get(url, timeout=self.config.timeout_seconds, headers=headers)
                response.raise_for_status()
                return response
            except requests.HTTPError as exc:
                last_exc = exc
                status = exc.response.status_code if exc.response is not None else None
                non_retryable_client_error = status is not None and 400 <= status < 500 and status not in {408, 429}
                if non_retryable_client_error:
                    if not suppress_non_retryable_log:
                        LOGGER.warning(
                            "Request failed (non-retryable %s) url=%s error=%s",
                            status,
                            url,
                            exc,
                        )
                    break
                if attempt == self.config.retries:
                    break
                sleep_seconds = self.config.backoff_seconds * attempt
                LOGGER.warning(
                    "Request failed (%s/%s) url=%s error=%s; retrying in %.1fs",
                    attempt,
                    self.config.retries,
                    url,
                    exc,
                    sleep_seconds,
                )
                time.sleep(sleep_seconds)
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                if attempt == self.config.retries:
                    break
                sleep_seconds = self.config.backoff_seconds * attempt
                LOGGER.warning(
                    "Request failed (%s/%s) url=%s error=%s; retrying in %.1fs",
                    attempt,
                    self.config.retries,
                    url,
                    exc,
                    sleep_seconds,
                )
                time.sleep(sleep_seconds)

        assert last_exc is not None
        raise RuntimeError(f"Request failed for {url}") from last_exc
