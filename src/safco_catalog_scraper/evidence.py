"""Read saved evidence and resolve agent-selected locations without executing page code."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any, Callable

import httpx
from bs4 import BeautifulSoup

from .fetching import validate_request
from .recovery import RecoveryActionError, RecoveryPolicy, RecoveryResult
from .storage import Storage

_ASSIGNMENT = re.compile(r"(?:^|[;\n])\s*window\.([A-Za-z_$][\w$]*)\s*=\s*(?:JSON\.parse\(\s*)?")
_SECRET_KEYS = {"apikey", "api_key", "authorization", "access_token", "password", "token"}


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Ambiguous evidence JSON")
        result[key] = value
    return result


def _constant(_: str) -> Any:
    raise ValueError("Nonfinite evidence number")


def _roots(body: bytes) -> dict[str, Any]:
    decoder = json.JSONDecoder(object_pairs_hook=_pairs, parse_float=str,
                               parse_constant=_constant)
    roots = {}
    try:
        roots["body"] = decoder.decode(body.decode("utf-8"))
        return roots
    except (ValueError, UnicodeError, RecursionError):
        pass
    for index, script in enumerate(BeautifulSoup(body, "html.parser").find_all("script")):
        text = script.string or ""
        starts = [("script:" + str(index), text.lstrip(), False, True)] if script.get("type") == "application/json" else []
        starts.extend((f"script:{index}:{match[1]}", text[match.end():].lstrip(),
                       "JSON.parse(" in match[0], False)
                      for match in _ASSIGNMENT.finditer(text))
        for locator, value, wrapped, document in starts:
            try:
                decoded, end = decoder.raw_decode(value)
                tail = value[end:].lstrip()
                if wrapped:
                    if not isinstance(decoded, str) or not tail.startswith(")"):
                        raise ValueError("Invalid JSON.parse literal")
                    tail = tail[1:].lstrip()
                if tail and (document or not tail.startswith(";")):
                    raise ValueError("Evidence requires a JSON literal without expressions")
                for _ in range(2):
                    if not isinstance(decoded, str):
                        break
                    decoded = decoder.decode(decoded)
                roots[locator] = None if locator in roots else decoded
            except (ValueError, RecursionError):
                continue
    return roots


def _resolve(body: bytes, locator: str) -> Any:
    root, _, pointer = locator.partition("#")
    value = _roots(body)[root]
    if pointer and not pointer.startswith("/"):
        raise ValueError("Expected a JSON pointer after #")
    for token in pointer.split("/")[1:] if pointer else ():
        if re.search(r"~(?![01])", token):
            raise ValueError("Invalid JSON pointer escape")
        token = token.replace("~1", "/").replace("~0", "~")
        if isinstance(value, list):
            if re.fullmatch(r"0|[1-9][0-9]*", token) is None:
                raise ValueError("Invalid array index")
            value = value[int(token)]
        else:
            value = value[token]
    return value


def _redact(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: "[redacted]" if key.casefold() in _SECRET_KEYS else _redact(item)
                for key, item in value.items()}
    if isinstance(value, list):
        return [_redact(item) for item in value]
    return value


class EvidenceTools:
    """Execute bounded reads or same-task HTTP requests; never accept catalog data.

    JSON locations use body#/pointer or script:INDEX[:ASSIGNMENT]#/pointer.
    Scripts view inventories locations; JSON view reads their values. No page
    JavaScript, model-generated code, arbitrary files or browser actions execute.
    Authentication comes from the runner, outside observations and model inputs.
    """

    def __init__(self, store: Storage, task_id: int, policy: RecoveryPolicy, *,
                 max_response_bytes: int, request_interval_seconds: float = 0,
                 authentication: Callable[[dict[str, Any]], dict[str, str]] | None = None,
                 transport: httpx.AsyncBaseTransport | None = None):
        self.store, self.task_id, self.policy = store, task_id, policy
        self.max_response_bytes, self.interval = max_response_bytes, request_interval_seconds
        self.authentication, self.transport = authentication, transport

    def source(self, source_id: int) -> dict[str, Any]:
        """Load intact successful evidence owned by this task; reject other sources."""
        event = self.store.get_event(source_id)
        if (event["task_id"] != self.task_id or event["type"] != "fetch"
                or event["status"] != "succeeded" or event["body"] is None
                or event["details"].get("body_complete") is not True
                or not 200 <= event["details"].get("http_status", 0) < 300
                or hashlib.sha256(event["body"]).hexdigest() != event["content_hash"]):
            raise ValueError("Invalid task evidence")
        return event

    def resolve_location(self, source_id: int, locator: str) -> Any:
        """Resolve a saved JSON location from intact evidence on this task."""
        return _resolve(self.source(source_id)["body"], locator)

    async def __call__(self, request: dict[str, Any], timeout_seconds: float) -> dict[str, Any]:
        """Execute one read_source/fetch_evidence action inside the caller's timeout."""
        if request["task_id"] != self.task_id:
            raise RecoveryActionError("evidence_task_mismatch", uncertain=False)
        action, args = request["action"], request["arguments"]
        if action == "fetch_evidence":
            return await self.fetch(args["request"], timeout_seconds)
        if action != "read_source":
            raise RecoveryActionError("browser_unavailable", uncertain=False)
        try:
            source = self.source(args["source_event_id"])
            body, view = source["body"], args["view"]
            locator = args.get("locator", "body")
            if view == "scripts":
                value = [{"locator": root, "type": type(item).__name__,
                          "keys": list(item)[:30] if isinstance(item, dict) else None}
                         for root, item in _roots(body).items()]
            elif view == "json":
                value = _redact(_resolve(body, locator))
            else:
                soup = BeautifulSoup(body, "html.parser")
                for node in soup(["script", "style"]):
                    node.decompose()
                value = ([table.get_text(" ", strip=True) for table in soup.select("table")]
                         if view == "tables" else soup.get_text(" ", strip=True))
            encoded = json.dumps(value, ensure_ascii=False, allow_nan=False).encode()
            offset = args.get("offset", 0)
            limit = min(args.get("limit_bytes", self.policy.max_observation_bytes),
                        self.policy.max_observation_bytes // 2)
            content = encoded[offset:offset + limit].decode("utf-8", errors="ignore")
            return {"source_event_id": source["event_id"], "locator": locator,
                    "offset": offset, "total_bytes": len(encoded), "content": content,
                    "truncated": offset > 0 or offset + limit < len(encoded)}
        except (ValueError, KeyError, IndexError, TypeError, RecursionError):
            raise RecoveryActionError("evidence_location_invalid", uncertain=False) from None

    async def fetch(self, request: dict[str, Any], timeout: float) -> dict[str, Any]:
        """Save one same-task HTTP attempt and Retry-After for controller scheduling.

        Do not follow redirects, retry the request or promote the primary source.
        """
        method, url, payload = validate_request(request)
        if request != self.store.get_task(self.task_id)["request"]:
            raise RecoveryActionError("evidence_request_mismatch", uncertain=False)
        headers = self.authentication(request) if self.authentication else {}
        await asyncio.sleep(self.interval)
        event = self.store.start_event(self.task_id, "fetch", details={"request": request, "recovery_evidence": True})
        data, status = bytearray(), None
        response_headers = {}
        try:
            async with httpx.AsyncClient(transport=self.transport, trust_env=False,
                                         follow_redirects=False, timeout=timeout) as client:
                async with client.stream(method, url, json=payload, headers=headers) as response:
                    status = response.status_code
                    if "retry-after" in response.headers:
                        response_headers["retry-after"] = response.headers["retry-after"]
                    async for chunk in response.aiter_bytes(chunk_size=4096):
                        if len(data) + len(chunk) > self.max_response_bytes:
                            raise RecoveryActionError("evidence_response_limit", uncertain=False)
                        data.extend(chunk)
        except asyncio.CancelledError:
            self.store.update_event(event, "uncertain", body=bytes(data), details={
                "http_status": status, "response_headers": response_headers,
                "body_complete": False, "error_code": "evidence_cancelled"})
            raise
        except (httpx.RequestError, RecoveryActionError) as error:
            self.store.update_event(event, "failed", body=bytes(data), details={
                "http_status": status, "response_headers": response_headers,
                "body_complete": False, "error_code": "evidence_fetch_failed"})
            if isinstance(error, RecoveryActionError):
                raise
            raise RecoveryActionError("evidence_fetch_failed") from None
        complete = status is not None and 200 <= status < 300
        self.store.update_event(event, "succeeded" if complete else "failed", body=bytes(data), details={
            "http_status": status, "response_headers": response_headers,
            "body_complete": True, "captured_at": datetime.now(timezone.utc).isoformat()})
        if not complete:
            raise RecoveryActionError("evidence_http_failed", uncertain=False)
        return {"source_event_id": event, "locator": "body", "saved_bytes": len(data)}

    def resolve(self, result: RecoveryResult) -> dict[str, tuple[Any, dict[str, Any], str]]:
        """Resolve proposed dataset locations against intact same-request source bytes.

        Records contain only kind=items/product_total/category_config/category_page.
        One reference locates each dataset; model-supplied catalog values are rejected.
        Return kind -> (source value, source event, locator). Callers still run the
        deterministic discovery/extraction validators before accepting any result.
        """
        observations = {}
        for event_id in result.event_ids:
            event = self.store.get_event(event_id)
            if event["task_id"] != self.task_id or event["type"] != "recovery":
                raise ValueError("Invalid recovery provenance")
            for step in event["details"]["recovery"]["steps"]:
                if step["kind"] == "evidence" and step["status"] == "succeeded":
                    observations[step["evidence_id"]] = step["result"].get("source_event_id")
        resolved = {}
        for candidate in result.candidates:
            record, refs = candidate["record"], candidate["evidence"]
            kind = record.get("kind")
            if set(record) != {"kind"} or kind not in {"items", "product_total", "category_config", "category_page"}:
                raise ValueError("Propose source locations, not invented catalog fields")
            if kind in resolved or len(refs) != 1:
                raise ValueError("Each dataset needs one unambiguous location")
            ref = refs[0]
            evidence_id = ref["evidence_id"]
            source_id = (int(evidence_id.removeprefix("source:")) if evidence_id.startswith("source:")
                         else observations.get(evidence_id))
            source = self.source(source_id)
            if source["details"].get("request") != self.store.get_task(self.task_id)["request"]:
                raise ValueError("Candidate source belongs to a different request")
            resolved[kind] = (_resolve(source["body"], ref["locator"]), source, ref["locator"])
        return resolved
