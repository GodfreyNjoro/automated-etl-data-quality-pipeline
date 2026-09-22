"""REST API extractor with authentication, rate limiting and pagination.

Supported ``options`` (from the source config):

    base_url:        Root URL of the API.
    endpoint:        Path appended to base_url.
    method:          HTTP method (default GET).
    auth:            {type: none|api_key|bearer|basic, ...}
    headers:         Extra static headers.
    params:          Static query parameters.
    rate_limit_per_sec: Max requests/second (token-bucket throttling).
    timeout:         Per-request timeout in seconds.
    pagination:      {type: none|page|offset|cursor, ...}
    records_path:    Dotted path to the list of records in the response body.
    max_pages:       Safety cap on pagination.

The extractor is resilient: transient HTTP errors are retried with exponential
backoff, and ``429 Too Many Requests`` responses honour any ``Retry-After``
header.
"""

from __future__ import annotations

import time
from typing import Any

import pandas as pd
import requests

from src.extractors.base import BaseExtractor, ExtractionResult


class RateLimiter:
    """Simple token-bucket rate limiter (requests per second)."""

    def __init__(self, rate_per_sec: float | None) -> None:
        self.min_interval = 1.0 / rate_per_sec if rate_per_sec else 0.0
        self._last = 0.0

    def wait(self) -> None:
        if self.min_interval <= 0:
            return
        elapsed = time.monotonic() - self._last
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self._last = time.monotonic()


class RestApiExtractor(BaseExtractor):
    """Extract records from a paginated, authenticated REST API."""

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        headers: dict[str, str] = dict(self.options.get("headers", {}))
        auth = self.options.get("auth", {"type": "none"})
        auth_type = auth.get("type", "none")

        if auth_type == "api_key":
            header_name = auth.get("header", "X-API-Key")
            headers[header_name] = auth.get("value", "")
        elif auth_type == "bearer":
            headers["Authorization"] = f"Bearer {auth.get('token', '')}"
        elif auth_type == "basic":
            session.auth = (auth.get("username", ""), auth.get("password", ""))

        session.headers.update(headers)
        return session

    @staticmethod
    def _dig(body: Any, path: str | None) -> Any:
        """Follow a dotted ``records_path`` into a JSON body."""
        if not path:
            return body
        current = body
        for part in path.split("."):
            if isinstance(current, dict):
                current = current.get(part, [])
            else:
                return []
        return current

    def _request(
        self, session: requests.Session, url: str, params: dict[str, Any]
    ) -> dict[str, Any]:
        method = self.options.get("method", "GET").upper()
        timeout = self.options.get("timeout", 30)
        max_retries = int(self.options.get("max_retries", 3))
        backoff = float(self.options.get("retry_backoff_seconds", 2.0))

        last_exc: Exception | None = None
        for attempt in range(max_retries + 1):
            try:
                resp = session.request(method, url, params=params, timeout=timeout)
                if resp.status_code == 429:
                    retry_after = float(resp.headers.get("Retry-After", backoff * (2**attempt)))
                    self.log.warning(
                        "Rate limited; backing off",
                        extra={"retry_after": retry_after},
                    )
                    time.sleep(retry_after)
                    continue
                resp.raise_for_status()
                return resp.json()
            except (requests.RequestException, ValueError) as exc:
                last_exc = exc
                if attempt < max_retries:
                    sleep_for = backoff * (2**attempt)
                    self.log.warning(
                        "Request failed; retrying",
                        extra={"attempt": attempt + 1, "sleep": sleep_for, "error": str(exc)},
                    )
                    time.sleep(sleep_for)
                else:
                    raise
        raise RuntimeError(f"REST extraction failed: {last_exc}")

    def extract(self, since: Any | None = None) -> ExtractionResult:
        session = self._build_session()
        limiter = RateLimiter(self.options.get("rate_limit_per_sec"))
        base_url = self.options["base_url"].rstrip("/")
        endpoint = self.options.get("endpoint", "").lstrip("/")
        url = f"{base_url}/{endpoint}" if endpoint else base_url

        params: dict[str, Any] = dict(self.options.get("params", {}))
        if since is not None and self.options.get("incremental_param"):
            params[self.options["incremental_param"]] = since

        pagination = self.options.get("pagination", {"type": "none"})
        page_type = pagination.get("type", "none")
        records_path = self.options.get("records_path")
        max_pages = int(self.options.get("max_pages", 1000))

        all_records: list[dict[str, Any]] = []
        page = pagination.get("start_page", 1)
        offset = 0
        cursor: Any = None
        pages_fetched = 0

        page_size = pagination.get("page_size", 100)
        while pages_fetched < max_pages:
            page_params = dict(params)
            if page_type == "page":
                page_params[pagination.get("page_param", "page")] = page
                page_params[pagination.get("size_param", "per_page")] = page_size
            elif page_type == "offset":
                page_params[pagination.get("offset_param", "offset")] = offset
                page_params[pagination.get("limit_param", "limit")] = page_size
            elif page_type == "cursor" and cursor is not None:
                page_params[pagination.get("cursor_param", "cursor")] = cursor

            limiter.wait()
            body = self._request(session, url, page_params)
            records = self._dig(body, records_path)
            if not isinstance(records, list):
                records = [records] if records else []
            all_records.extend(records)
            pages_fetched += 1

            if page_type == "none" or not records:
                break
            if page_type == "page":
                page += 1
            elif page_type == "offset":
                offset += pagination.get("page_size", 100)
                if len(records) < pagination.get("page_size", 100):
                    break
            elif page_type == "cursor":
                cursor = self._dig(body, pagination.get("next_cursor_path", ""))
                if not cursor:
                    break

        frame = pd.json_normalize(all_records) if all_records else pd.DataFrame()
        self.log.info(
            "REST extraction complete",
            extra={"source": self.config.name, "rows": len(frame), "pages": pages_fetched},
        )
        return ExtractionResult.from_frame(self.config, frame, pages=pages_fetched, url=url)

    def test_connection(self) -> bool:
        try:
            session = self._build_session()
            base_url = self.options["base_url"].rstrip("/")
            resp = session.get(base_url, timeout=self.options.get("timeout", 10))
            return resp.status_code < 500
        except requests.RequestException:
            return False
