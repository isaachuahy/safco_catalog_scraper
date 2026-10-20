"""Make one authenticated decision request to the hosted recovery endpoint."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import re
from collections.abc import Mapping
from time import monotonic
from typing import Any
from urllib.parse import unquote, urlsplit

import httpx

from .recovery import RecoveryActionError

logger = logging.getLogger(__name__)


def validate_endpoint(value: Any) -> str:
    """Return a normalized full HTTPS decision URL or reject unsafe configuration."""
    if (not isinstance(value, str) or not value or len(value) > 8192
            or "?" in value or "#" in value
            or any(ord(char) <= 32 or ord(char) == 127 for char in value)):
        raise ValueError("Supply the full HTTPS recovery endpoint without query parameters or fragments")
    try:
        parts = urlsplit(value)
        valid = (parts.scheme == "https" and bool(parts.hostname) and bool(parts.path.strip("/"))
                 and parts.port in (None, 443) and parts.username is None and parts.password is None
                 and "\\" not in value
                 and not any(part in (".", "..") for part in unquote(parts.path).split("/")))
        if valid:
            return str(httpx.URL(value))
    except (ValueError, httpx.InvalidURL):
        pass
    raise ValueError("Supply a valid HTTPS recovery endpoint without URL credentials")


def _object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate response JSON key")
        result[key] = value
    return result


def _number(value: str) -> float:
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("Nonfinite response number")
    return number


def _constant(value: str) -> Any:
    raise ValueError("Nonstandard response JSON constant")


def _decode(body: bytes) -> dict[str, Any]:
    try:
        value = json.loads(body.decode("utf-8"), object_pairs_hook=_object,
                           parse_float=_number, parse_constant=_constant)
        if isinstance(value, dict):
            return value
    except (ValueError, UnicodeError, RecursionError):
        pass
    raise RecoveryActionError("service_invalid_json") from None


def _service_failure(value: dict[str, Any], request_id: str, status: int) -> RecoveryActionError:
    error = value.get("error")
    if (value.get("request_id") == request_id and isinstance(error, dict)
            and isinstance(error.get("code"), str)
            and re.fullmatch(r"[a-z][a-z0-9_]{0,63}", error["code"]) is not None):
        return RecoveryActionError("service_" + error["code"][:56],
                                   uncertain=error.get("inference_started") is not False)
    return RecoveryActionError("service_http_" + str(status))


class RecoveryClient:
    """Own one sequential async connection pool for the configured recovery URL.

    Pass this instance as run_recovery's decide callback. The caller obtains the
    endpoint URL and optional access token from its execution environment; this
    client never reads environment credentials or a model-provider API key.
    The URL is fixed at construction, TLS verification is enabled, redirects and
    environment proxies/netrc are disabled, and cookies do not carry between calls.

    There is no automatic retry or remote idempotency guarantee. X-Request-ID is
    only for correlation. The coordinator owns durable replay, budget reservations
    and validation of response identities, decisions and evidence references.
    Inject only a trusted single-attempt async transport for offline tests or
    explicitly configured networking. This instance owns and closes that transport.
    Use the client on one event loop inside an async context manager.
    """

    def __init__(self, endpoint_url: str, access_token: str | None = None, *,
                 max_request_bytes: int = 64 * 1024,
                 max_response_bytes: int = 64 * 1024,
                 transport: httpx.AsyncBaseTransport | None = None) -> None:
        """Validate the HTTPS endpoint, optional token and byte limits; send nothing.

        endpoint_url is the full route, for example
        https://your-project.vercel.app/recovery/decision. Byte limits count UTF-8
        JSON, not HTTP headers. Supply the same limits as the saved recovery policy.
        Omit access_token for a public demo endpoint. A supplied token is sent as
        a bearer credential and never enters callback results. Server policy
        decides whether authentication is required; the client never retries a
        rejected authenticated request anonymously.
        """
        self._url = validate_endpoint(endpoint_url)
        if access_token is not None and (not isinstance(access_token, str) or len(access_token) > 4096
                or re.fullmatch(r"[A-Za-z0-9._~+/-]+=*", access_token) is None):
            raise ValueError("Supply a nonempty bearer token without whitespace or a Bearer prefix")
        for name, value in (("max_request_bytes", max_request_bytes),
                            ("max_response_bytes", max_response_bytes)):
            if type(value) is not int or value < 1:
                raise ValueError(name + " must be a positive integer")
        self._max_request_bytes, self._max_response_bytes = max_request_bytes, max_response_bytes
        headers = {"Content-Type": "application/json", "Accept": "application/json",
                   "Accept-Encoding": "identity", "User-Agent": "SafcoCatalogScraper/0.1.0"}
        if access_token is not None:
            headers["Authorization"] = "Bearer " + access_token
        self._client = httpx.AsyncClient(
            transport=transport if transport is not None else httpx.AsyncHTTPTransport(
                retries=0, trust_env=False,
                limits=httpx.Limits(max_connections=1, max_keepalive_connections=1)),
            trust_env=False, follow_redirects=False,
            headers=headers)

    async def __aenter__(self) -> RecoveryClient:
        """Return the open client without making a request."""
        if self._client.is_closed:
            raise RuntimeError("Recovery client is closed")
        return self

    async def __aexit__(self, *_: Any) -> None:
        """Close connections without suppressing errors or cancellation."""
        await self.aclose()

    async def aclose(self) -> None:
        """Release the owned client/transport; repeated close calls are safe."""
        await self._client.aclose()

    async def __call__(self, request: dict[str, Any], timeout_seconds: float) -> dict[str, Any]:
        """POST one bounded decision request and return a JSON object.

        Timeout covers the complete exchange, in addition to HTTPX phase limits.
        Responses must be UTF-8 JSON with identity/no content encoding; model
        envelope validation belongs to the recovery coordinator. HTTP error bodies
        are inspected only for a matching request_id and
        error={code: <safe identifier>, inference_started: false}. Only that explicit
        preflight rejection, or a connection/pool failure before sending, proves
        inference did not start. Other service/response failures are uncertain.

        Raise ValueError for invalid caller input, RuntimeError after close, and
        RecoveryActionError for expected service failures. Error codes and logs
        omit credentials, response text and HTTP exception messages. Cancellation
        and unexpected programmer errors propagate. Never retry this POST here.
        """
        if self._client.is_closed:
            raise RuntimeError("Recovery client is closed")
        if (type(timeout_seconds) not in (int, float) or not math.isfinite(timeout_seconds)
                or timeout_seconds <= 0):
            raise ValueError("timeout_seconds must be finite and positive")
        if not isinstance(request, Mapping):
            raise ValueError("Supply a JSON request object")
        request_id = request.get("request_id")
        if not isinstance(request_id, str) or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,199}", request_id) is None:
            raise ValueError("Supply a nonempty, bounded ASCII request_id for correlation")
        try:
            body = json.dumps(dict(request), sort_keys=True, separators=(",", ":"),
                              ensure_ascii=False, allow_nan=False).encode("utf-8")
        except (ValueError, TypeError, UnicodeError, RecursionError):
            raise ValueError("Recovery request must be finite UTF-8 JSON") from None
        if len(body) > self._max_request_bytes:
            raise RecoveryActionError("service_request_limit", uncertain=False)
        started = monotonic()
        logger.info("Recovery HTTP started request_id=%s", request_id)
        self._client.cookies.clear()
        try:
            value = await self._post(body, request_id, timeout_seconds)
        except RecoveryActionError as error:
            logger.warning("Recovery HTTP failed request_id=%s code=%s uncertain=%s elapsed_ms=%d",
                           request_id, error.code, error.uncertain, int((monotonic() - started) * 1000))
            raise
        except asyncio.CancelledError:
            logger.warning("Recovery HTTP cancelled request_id=%s uncertain=true", request_id)
            raise
        logger.info("Recovery HTTP completed request_id=%s elapsed_ms=%d",
                    request_id, int((monotonic() - started) * 1000))
        return value

    async def _post(self, body: bytes, request_id: str, timeout: float) -> dict[str, Any]:
        try:
            async with asyncio.timeout(timeout):
                async with self._client.stream("POST", self._url, content=body,
                                               headers={"X-Request-ID": request_id},
                                               timeout=httpx.Timeout(timeout)) as response:
                    status = response.status_code
                    success = 200 <= status < 300
                    if 300 <= status < 400:
                        raise RecoveryActionError("service_redirect")
                    encoding = response.headers.get("content-encoding", "identity").strip().lower()
                    if encoding != "identity":
                        raise RecoveryActionError("service_response_encoding")
                    content_type = response.headers.get("content-type", "").split(";", 1)[0].strip().lower()
                    if content_type != "application/json" and not (
                            content_type.startswith("application/") and content_type.endswith("+json")):
                        raise RecoveryActionError("service_content_type" if success else "service_http_" + str(status))
                    data = bytearray()
                    async for chunk in response.aiter_bytes(chunk_size=min(4096, self._max_response_bytes + 1)):
                        if len(data) + len(chunk) > self._max_response_bytes:
                            raise RecoveryActionError("service_response_limit")
                        data.extend(chunk)
                    try:
                        value = _decode(bytes(data))
                    except RecoveryActionError:
                        if not success:
                            raise RecoveryActionError("service_http_" + str(status)) from None
                        raise
                    if not success:
                        raise _service_failure(value, request_id, status)
                    return value
        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout):
            raise RecoveryActionError("service_connection_failed", uncertain=False) from None
        except (httpx.TimeoutException, TimeoutError):
            raise RecoveryActionError("service_timeout") from None
        except (httpx.LocalProtocolError, httpx.UnsupportedProtocol):
            raise
        except httpx.RequestError:
            raise RecoveryActionError("service_transport_error") from None
