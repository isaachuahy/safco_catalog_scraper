"""Fetch one public Safco observation; scheduling and persistence belong to the controller."""

from __future__ import annotations

import json
import logging
import math
import re
import ssl
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from time import monotonic
from typing import Any
from urllib.parse import unquote, urlsplit

import httpx

logger = logging.getLogger(__name__)

_RETRYABLE_STATUSES = {408, 429, 500, 502, 503, 504}
_RESPONSE_HEADERS = ("content-type", "content-encoding", "content-length", "date",
                     "retry-after", "location")
_AUTH_HEADERS = {"x-algolia-application-id", "x-algolia-api-key"}


@dataclass(frozen=True)
class FetchResult:
    """One attempt's evidence and scheduling hints, without request credentials.

    body contains decoded response bytes, possibly a bounded prefix on failure.
    Only succeeded results may enter ordinary extraction. body_complete describes
    receipt of the body, independently of HTTP success. captured_at is UTC at the
    end of the attempt; elapsed_ms uses a monotonic clock. headers contains only
    selected response headers; location is untrusted and is never followed here.
    """

    status_code: int | None
    body: bytes = field(repr=False)
    body_complete: bool
    captured_at: str
    elapsed_ms: int
    headers: dict[str, str] = field(repr=False)
    error_code: str | None = None
    error_type: str | None = None
    retryable: bool = False

    @property
    def succeeded(self) -> bool:
        """Indicate a fully received 2xx response without a collection error."""
        return (self.status_code is not None and 200 <= self.status_code < 300
                and self.body_complete and self.error_code is None)

    @property
    def retry_after(self) -> str | None:
        """Return the server's raw delay/date; the controller validates and schedules it."""
        return self.headers.get("retry-after")

    @property
    def details(self) -> dict[str, Any]:
        """Return JSON-safe fetch-event metadata; persist body separately as evidence.

        saved_bytes counts decoded bytes, so it may differ from the source
        Content-Length when the server compresses its response.
        """
        return {"http_status": self.status_code, "captured_at": self.captured_at,
                "elapsed_ms": self.elapsed_ms, "response_headers": dict(self.headers),
                "saved_bytes": len(self.body), "body_complete": self.body_complete,
                "error_code": self.error_code, "error_type": self.error_type,
                "retryable": self.retryable}


def validate_request(request: Mapping[str, Any]) -> tuple[str, str, Any]:
    """Validate public Safco/Algolia request scope without handling credentials.

    Return method, URL and optional JSON. Collectors and recovery tools share this
    policy; execution must separately validate Algolia authentication headers.
    """
    if not isinstance(request, Mapping) or request.keys() - {
            "method", "url", "json", "category_url", "page"}:
        raise ValueError("Supply a task request with method/url and optional JSON/category metadata")
    method, url = request.get("method"), request.get("url")
    if method not in ("GET", "POST") or not isinstance(url, str):
        raise ValueError("Supply an explicit GET or POST and a public HTTPS URL")
    try:
        parts = urlsplit(url)
        valid = (parts.scheme == "https" and parts.port in (None, 443)
                 and parts.username is None and parts.password is None and not parts.fragment
                 and not any(part in (".", "..") for part in unquote(parts.path).split("/"))
                 and not any(ord(char) <= 32 or ord(char) == 127 for char in url))
    except ValueError:
        valid = False
    if not valid:
        raise ValueError("Expected a public HTTPS URL without credentials or a fragment")
    if method == "GET":
        valid = (parts.hostname == "www.safcodental.com" and "json" not in request
                 and any(parts.path.startswith(prefix) and len(parts.path) > len(prefix)
                         for prefix in ("/catalog/", "/product/")))
    else:
        valid = (re.fullmatch(r"[a-z0-9]+-dsn\.algolia\.net", parts.hostname or "", re.I) is not None
                 and parts.path == "/1/indexes/*/queries" and not parts.query
                 and isinstance(request.get("json"), Mapping))
        if valid:
            try:
                json.dumps(request["json"], allow_nan=False)
            except (TypeError, ValueError, RecursionError):
                valid = False
    if not valid:
        raise ValueError("Expected a Safco catalog/product GET or an Algolia query POST")
    return method, url, request.get("json")


def _request_inputs(request: Mapping[str, Any], headers: Mapping[str, str] | None
                    ) -> tuple[str, str, dict[str, str], Any]:
    method, url, payload = validate_request(request)
    if headers is not None and not isinstance(headers, Mapping):
        raise ValueError("Authentication headers must be a separate mapping")
    auth = {}
    for key, value in (headers or {}).items():
        if (not isinstance(key, str) or key.lower() not in _AUTH_HEADERS or key.lower() in auth
                or not isinstance(value, str) or not value or not value.isascii()
                or any(ord(char) <= 32 or ord(char) == 127 for char in value)):
            raise ValueError("Only nonempty, distinct Algolia authentication headers are supported")
        auth[key.lower()] = value
    if method == "GET":
        valid = not auth
    else:
        application = (urlsplit(url).hostname or "").removesuffix("-dsn.algolia.net")
        valid = (set(auth) == _AUTH_HEADERS
                 and auth["x-algolia-application-id"].lower() == application)
    if not valid:
        raise ValueError("Expected a Safco catalog/product GET or a matching authenticated Algolia query POST")
    return method, url, auth, payload


def _request_failure(error: httpx.RequestError) -> tuple[str, bool]:
    cause: BaseException | None = error
    seen = set()
    while cause is not None and id(cause) not in seen:
        if isinstance(cause, ssl.SSLCertVerificationError):
            return "certificate_verification", False
        seen.add(id(cause))
        cause = cause.__cause__ or cause.__context__
    for error_class, code in ((httpx.TimeoutException, "timeout"),
                              (httpx.NetworkError, "network_error"),
                              (httpx.RemoteProtocolError, "remote_protocol_error")):
        if isinstance(error, error_class):
            return code, True
    return "request_error", False


class Fetcher:
    """Reuse one synchronous HTTP client from one collector thread.

    timeout_seconds bounds inactivity in each HTTPX phase, not total wall time.
    max_response_bytes caps retained decoded bytes, including error bodies. The
    default 8 MiB exceeds the 1.55 MB largest product response in the saved audit.
    Direct HTTPS verifies certificates; environment credentials/proxies and cookie
    sessions are disabled. Inject a trusted single-attempt transport for offline
    checks or explicitly configured networking. This instance owns that transport.
    """

    def __init__(self, *, timeout_seconds: float = 30.0,
                 max_response_bytes: int = 8 * 1024 * 1024,
                 transport: httpx.BaseTransport | None = None) -> None:
        """Configure finite phase timeouts and a positive response limit; send no requests."""
        if (type(timeout_seconds) not in (int, float) or not math.isfinite(timeout_seconds)
                or timeout_seconds <= 0):
            raise ValueError("timeout_seconds must be finite and positive")
        if type(max_response_bytes) is not int or max_response_bytes < 1:
            raise ValueError("max_response_bytes must be a positive integer")
        self._max_response_bytes = max_response_bytes
        self._client = httpx.Client(
            transport=transport if transport is not None else httpx.HTTPTransport(
                retries=0, trust_env=False,
                limits=httpx.Limits(max_connections=1, max_keepalive_connections=1)),
            timeout=httpx.Timeout(timeout_seconds), trust_env=False, follow_redirects=False,
            headers={"User-Agent": "SafcoCatalogScraper/0.1.0", "Accept-Encoding": "identity"})

    def __enter__(self) -> Fetcher:
        """Return the collector-owned fetcher."""
        return self

    def __exit__(self, *_: Any) -> None:
        """Close the client and its transport when the context ends."""
        self.close()

    def close(self) -> None:
        """Release network resources; repeated close calls are safe."""
        self._client.close()

    def fetch(self, request: Mapping[str, Any], *,
              headers: Mapping[str, str] | None = None) -> FetchResult:
        """Execute exactly one validated task request without retries or redirects.

        Accept TaskSpec.request, including ignored category_url/page metadata.
        Pass CategoryConfig.authentication_headers separately for an Algolia POST.
        Request authentication headers never enter the result or this logger.
        Return bounded evidence for HTTP and transport failures; retain partial
        bytes without presenting them as a successfully collected document.
        Report timeouts/network/remote-protocol failures and selected HTTP statuses
        as retryable hints. The controller decides limits, delays and persistence.
        Raise ValueError for invalid caller input and RuntimeError after close;
        programmer errors propagate instead of becoming source/recovery failures.
        """
        if self._client.is_closed:
            raise RuntimeError("Fetcher is closed")
        method, url, auth, payload = _request_inputs(request, headers)
        parts = urlsplit(url)
        target = f"https://{parts.hostname}{parts.path}"
        started = monotonic()
        logger.info("HTTP fetch started method=%s target=%s", method, target)
        status = None
        response_headers = {}
        body = bytearray()
        complete = False
        error_code = error_type = None
        retryable = False
        # Guest collection must not inherit cookies from an earlier product/category.
        self._client.cookies.clear()
        try:
            with self._client.stream(method, url, headers=auth, json=payload) as response:
                status = response.status_code
                response_headers = {name: response.headers[name] for name in _RESPONSE_HEADERS
                                    if name in response.headers}
                for chunk in response.iter_bytes():
                    remaining = self._max_response_bytes - len(body)
                    body.extend(chunk[:remaining])
                    if len(chunk) > remaining:
                        error_code = "response_too_large"
                        break
                else:
                    complete = True
            if error_code is None and not 200 <= status < 300:
                error_code = "redirect" if 300 <= status < 400 else "http_status"
                retryable = status in _RETRYABLE_STATUSES
            elif error_code == "response_too_large":
                retryable = status in _RETRYABLE_STATUSES
        except (httpx.LocalProtocolError, httpx.UnsupportedProtocol):
            raise
        except httpx.RequestError as error:
            error_type = type(error).__name__
            error_code, retryable = _request_failure(error)
            if status is not None and not 200 <= status < 300 and status not in _RETRYABLE_STATUSES:
                retryable = False
        result = FetchResult(status, bytes(body), complete,
                             datetime.now(timezone.utc).isoformat(timespec="microseconds"),
                             max(0, int((monotonic() - started) * 1000)), response_headers,
                             error_code, error_type, retryable)
        log = logger.info if result.succeeded else logger.warning
        log("HTTP fetch finished method=%s target=%s status=%s saved_bytes=%d complete=%s "
            "elapsed_ms=%d error=%s error_type=%s retryable=%s", method, target, status,
            len(body), complete, result.elapsed_ms, error_code, error_type, retryable)
        return result
