"""Schedule Safco collection, bounded recovery and durable catalog acceptance."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import sqlite3
import sys
import time
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from importlib.metadata import version
from pathlib import Path
from typing import Any

import httpx

from .discovery import CategoryConfig, DiscoveryError, category_task, decode_page, parse_category_config
from .diagnostics import failure_location
from .evidence import EvidenceTools
from .extraction import ProductExtraction, ExtractionError, extract_product, finalize_product, reported_item_count
from .fetching import Fetcher
from .recovery import RecoveryCall, RecoveryCase, RecoveryPolicy, run_recovery
from .recovery_client import RecoveryClient, validate_endpoint
from .storage import Storage

logger = logging.getLogger(__name__)

DEFAULT_CATEGORIES = ("https://www.safcodental.com/catalog/gloves",
                      "https://www.safcodental.com/catalog/sutures-surgical-products")
RECOVERY_DEFERRED_REASON = "Recovery is not configured for this run"
_UNRESOLVED = {"partial", "failed", "deferred"}


class RunSetupError(ValueError):
    """The requested run cannot start with these settings or saved state."""


@dataclass(frozen=True)
class RunConfig:
    """Non-secret settings saved unchanged for resumption.

    unresolved_product_limit is an optional absolute count; None attempts all products.
    max_fetch_attempts includes the first request and uncertain requests. Backoff
    is capped by retry_max_seconds; a valid later Retry-After is always honored.
    Request spacing is global to this sequential collector. HTTP timeouts apply
    per phase as documented by Fetcher. Optional hosted recovery keeps its endpoint
    and complete policy here; reviewer credentials remain outside saved settings.
    """

    unresolved_product_limit: int | None = None
    categories: tuple[str, ...] = DEFAULT_CATEGORIES
    max_fetch_attempts: int = 3
    request_interval_seconds: float = 0.5
    retry_base_seconds: float = 1.0
    retry_max_seconds: float = 60.0
    timeout_seconds: float = 30.0
    max_response_bytes: int = 8 * 1024 * 1024
    recovery_endpoint: str | None = None
    recovery_policy: RecoveryPolicy | None = None

    def __post_init__(self) -> None:
        if self.unresolved_product_limit is not None and (
                type(self.unresolved_product_limit) is not int or self.unresolved_product_limit < 1):
            raise ValueError("unresolved_product_limit must be a positive integer or None")
        for name in ("max_fetch_attempts", "max_response_bytes"):
            value = getattr(self, name)
            if type(value) is not int or value < 1:
                raise ValueError(name + " must be a positive integer")
        for name in ("request_interval_seconds", "retry_base_seconds", "retry_max_seconds", "timeout_seconds"):
            value = getattr(self, name)
            if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
                raise ValueError(name + " must be finite and nonnegative")
        if self.timeout_seconds == 0 or self.retry_max_seconds < self.retry_base_seconds:
            raise ValueError("Require a positive timeout and retry_max_seconds >= retry_base_seconds")
        if not isinstance(self.categories, (tuple, list)) or not self.categories:
            raise ValueError("Supply at least one Safco category")
        categories = tuple(category_task(url).request["url"] for url in self.categories)
        if len(set(categories)) != len(categories):
            raise ValueError("Categories must be distinct")
        object.__setattr__(self, "categories", categories)
        if (self.recovery_endpoint is None) != (self.recovery_policy is None):
            raise ValueError("Recovery requires both an endpoint and a saved policy")
        if self.recovery_policy is not None:
            if not isinstance(self.recovery_policy, RecoveryPolicy) or self.recovery_policy.browser_enabled:
                raise ValueError("Supply a RecoveryPolicy with browser recovery disabled")
            object.__setattr__(self, "recovery_endpoint", validate_endpoint(self.recovery_endpoint))

    def as_dict(self) -> dict[str, Any]:
        """Return the JSON-safe policy used for the saved configuration hash."""
        settings = asdict(self)
        settings.pop("recovery_policy")
        settings["categories"] = list(self.categories)
        settings["recovery"] = (self.recovery_policy.as_dict() if self.recovery_policy else
                                {"mode": "deferred", "reason": RECOVERY_DEFERRED_REASON})
        return settings


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _timestamp(value: str) -> datetime:
    timestamp = datetime.fromisoformat(value)
    if timestamp.utcoffset() != timedelta(0):
        raise ValueError("Saved timestamps must be UTC")
    return timestamp


def _code_version() -> str:
    digest = hashlib.sha256()
    root = Path(__file__).parent
    for name in ("__init__", "storage", "discovery", "extraction", "fetching", "runner",
                 "recovery", "recovery_client", "evidence", "diagnostics"):
        digest.update(name.encode() + b"\0" + (root / (name + ".py")).read_bytes())
    runtime = {"python": sys.version, "sqlite": sqlite3.sqlite_version,
               "httpx": version("httpx"), "beautifulsoup4": version("beautifulsoup4")}
    digest.update(json.dumps(runtime, sort_keys=True).encode())
    return digest.hexdigest()


def _retry_at(config: RunConfig, attempts: int, finished_at: str,
              retry_after: str | None) -> tuple[str, bool]:
    delay = config.retry_base_seconds
    for _ in range(attempts - 1):
        delay = min(config.retry_max_seconds, delay * 2)
        if delay == 0 or delay >= config.retry_max_seconds:
            break
    finished = _timestamp(finished_at)
    deadline = finished + timedelta(seconds=delay)
    invalid_header = False
    if retry_after is not None:
        try:
            text = retry_after.strip()
            if text.isascii() and text.isdecimal():
                server_deadline = finished + timedelta(seconds=int(text))
            else:
                server_deadline = parsedate_to_datetime(text)
                if server_deadline.utcoffset() is None:
                    server_deadline = server_deadline.replace(tzinfo=timezone.utc)
                server_deadline = server_deadline.astimezone(timezone.utc)
            deadline = max(deadline, server_deadline)
        except (ValueError, TypeError, OverflowError):
            invalid_header = True
    return deadline.isoformat(), invalid_header


class _Controller:
    def __init__(self, store: Storage, fetcher: Fetcher, run_id: str, config: RunConfig, *,
                 recovery_access_token: str | None = None, recovery_decide: RecoveryCall | None = None,
                 recovery_transport: httpx.AsyncBaseTransport | None = None):
        self.store, self.fetcher, self.run_id, self.config = store, fetcher, run_id, config
        self.category_configs: dict[str, CategoryConfig] = {}
        self.recovery_access_token, self.recovery_decide = recovery_access_token, recovery_decide
        self.recovery_transport = recovery_transport
        self.refresh_request_deadline()

    def refresh_request_deadline(self) -> None:
        fetches = [event for task in self.store.list_tasks(self.run_id)
                   for event in self.store.list_events(task["task_id"], include_bodies=False)
                   if event["type"] == "fetch"]
        times = [event["finished_at"] or event["started_at"] for event in fetches]
        self.next_request_at = max((_timestamp(value) for value in times), default=_now())
        if times:
            self.next_request_at += timedelta(seconds=self.config.request_interval_seconds)
        for event in fetches:
            details = event["details"]
            deadline = details.get("retry_decision", {}).get("server_not_before")
            if deadline is not None:
                self.next_request_at = max(self.next_request_at, _timestamp(deadline))
            retry_after = details.get("response_headers", {}).get("retry-after")
            if details.get("recovery_evidence") and retry_after is not None:
                deadline, invalid = _retry_at(self.config, 1, event["finished_at"] or event["started_at"], retry_after)
                if not invalid:
                    self.next_request_at = max(self.next_request_at, _timestamp(deadline))
                else:
                    logger.warning("Recovery Retry-After ignored run_id=%s event_id=%d reason=invalid_header",
                                   self.run_id, event["event_id"])

    def wait_until(self, deadline: datetime) -> None:
        while (remaining := (deadline - _now()).total_seconds()) > 0:
            if remaining >= 1:
                logger.info("Run waiting run_id=%s seconds=%.1f until=%s", self.run_id,
                            remaining, deadline.isoformat())
            time.sleep(min(remaining, 30.0))

    def saved_source(self, task: dict[str, Any]) -> dict[str, Any] | None:
        source_id = task["checkpoint"].get("source_event_id")
        if source_id is None:
            return None
        source = self.store.get_event(source_id)
        if (source["task_id"] != task["task_id"] or source["type"] != "fetch" or source["status"] != "succeeded"
                or source["body"] is None or source["details"].get("body_complete") is not True
                or not 200 <= source["details"].get("http_status", 0) < 300
                or hashlib.sha256(source["body"]).hexdigest() != source["content_hash"]):
            raise RuntimeError("Saved fetch checkpoint does not reference complete, intact evidence")
        _timestamp(source["details"]["captured_at"])
        return source

    def category_config(self, url: str) -> CategoryConfig:
        if url not in self.category_configs:
            seed = category_task(url)
            task = next(task for task in self.store.list_tasks(self.run_id)
                        if task["kind"] == "discovery" and task["target_key"] == seed.target_key)
            source = self.saved_source(task)
            if source is None:
                raise RuntimeError("Category API task has no saved configuration evidence")
            locator = task["checkpoint"].get("recovered_config_locator")
            self.category_configs[url] = parse_category_config(source["body"], url, source_config=(
                self.evidence_tools(task).resolve_location(source["event_id"], locator) if locator else None))
        return self.category_configs[url]

    def retry_decision(self, attempts: int, retryable: bool, finished_at: str,
                       retry_after: str | None = None) -> dict[str, Any]:
        retry = retryable and attempts < self.config.max_fetch_attempts
        deadline, invalid = _retry_at(self.config, attempts, finished_at, retry_after) if retryable else (None, False)
        return {"attempts": attempts, "retry": retry, "next_retry_at": deadline if retry else None,
                "server_not_before": deadline if retry_after is not None and not invalid else None,
                "reason": "retryable_failure" if retry else "attempt_limit" if retryable else "permanent_failure",
                "invalid_retry_after": invalid}

    def apply_fetch_failure(self, task: dict[str, Any], event: dict[str, Any]) -> None:
        details = event["details"]
        decision = details["retry_decision"]
        issue = {"code": "fetch_failed", "source_event_id": event["event_id"],
                 "http_status": details.get("http_status"), "error_code": details.get("error_code"),
                 "fetch_attempts": decision["attempts"], "reason": decision["reason"]}
        self.store.set_task_state(task["task_id"], "pending" if decision["retry"] else "failed",
                                  stage="fetch", issues=(issue,), next_retry_at=decision["next_retry_at"])
        logger.warning("Fetch decision run_id=%s task_id=%d event_id=%d attempts=%d retry=%s "
                       "next_retry_at=%s invalid_retry_after=%s", self.run_id, task["task_id"],
                       event["event_id"], decision["attempts"], decision["retry"],
                       decision["next_retry_at"], decision["invalid_retry_after"])

    def source_for(self, task: dict[str, Any]) -> dict[str, Any] | None:
        source = self.saved_source(task)
        if source is not None:
            logger.info("Reusing saved response run_id=%s task_id=%d source_event_id=%d",
                        self.run_id, task["task_id"], source["event_id"])
            return source
        events = [event for event in self.store.list_events(task["task_id"]) if event["type"] == "fetch"]
        if events:
            previous = events[-1]
            decision = previous["details"].get("retry_decision")
            if decision is None:
                if previous["status"] != "uncertain":
                    raise RuntimeError("Fetch evidence is missing its durable outcome/decision")
                decision = self.retry_decision(len(events), True, previous["finished_at"])
                self.store.update_event(previous["event_id"], "uncertain", details={
                    "error_code": "uncertain_request", "retryable": True, "retry_decision": decision})
                previous = self.store.list_events(task["task_id"])[-1]
                self.apply_fetch_failure(task, previous)
                return None
            if not decision["retry"] or _timestamp(decision["next_retry_at"]) > _now():
                self.apply_fetch_failure(task, previous)
                return None
        headers = None
        if task["request"]["method"] == "POST":
            headers = self.category_config(task["request"]["category_url"]).authentication_headers
        self.wait_until(self.next_request_at)
        event_id = self.store.start_event(task["task_id"], "fetch", details={"request": task["request"]})
        fetched = self.fetcher.fetch(task["request"], headers=headers)
        self.next_request_at = _timestamp(fetched.captured_at) + timedelta(seconds=self.config.request_interval_seconds)
        details = fetched.details
        if not fetched.succeeded:
            details["retry_decision"] = self.retry_decision(
                len(events) + 1, fetched.retryable, fetched.captured_at, fetched.retry_after)
            deadline = details["retry_decision"]["server_not_before"]
            if deadline is not None:
                self.next_request_at = max(self.next_request_at, _timestamp(deadline))
        self.store.update_event(event_id, "succeeded" if fetched.succeeded else "failed",
                                body=fetched.body, details=details,
                                checkpoint={"source_event_id": event_id} if fetched.succeeded else None)
        if not fetched.succeeded:
            self.apply_fetch_failure(task, self.store.list_events(task["task_id"])[-1])
            return None
        return self.saved_source(self.store.get_task(task["task_id"]))

    def defer_structure(self, task: dict[str, Any], event_id: int, source_id: int, code: str) -> None:
        issue = {"code": code, "source_event_id": source_id}
        self.store.update_event(event_id, "failed", details={
            "issues": [issue], "recovery_skipped_reason": RECOVERY_DEFERRED_REASON})
        self.store.set_task_state(task["task_id"], "deferred", stage="recovery", issues=(issue,))
        logger.warning("Recovery deferred run_id=%s task_id=%d source_event_id=%d code=%s reason=%s",
                       self.run_id, task["task_id"], source_id, code, RECOVERY_DEFERRED_REASON)

    def discover(self, task: dict[str, Any], source: dict[str, Any], event_id: int) -> None:
        try:
            if task["request"]["method"] == "GET":
                url = task["request"]["url"]
                config = parse_category_config(source["body"], url)
                self.category_configs[url] = config
                self.store.commit_discovery(event_id, (config.task(),), checkpoint={"category_url": url})
            else:
                request = task["request"]
                config = self.category_config(request["category_url"])
                if config.task(request["page"]).request != request:
                    raise DiscoveryError("API request differs from its saved category configuration")
                page = decode_page(source["body"], config, request["page"])
                self.store.commit_discovery(event_id, page.targets, checkpoint=page.checkpoint)
        except DiscoveryError:
            if self.config.recovery_policy:
                self.start_recovery(task, source, event_id, [{"code": "discovery_structure"}])
            else:
                self.defer_structure(task, event_id, source["event_id"], "discovery_structure")

    def evidence_tools(self, task: dict[str, Any]) -> EvidenceTools:
        def authenticate(request: dict[str, Any]) -> dict[str, str]:
            return (self.category_config(request["category_url"]).authentication_headers
                    if request["method"] == "POST" else {})
        return EvidenceTools(self.store, task["task_id"], self.config.recovery_policy,
                             max_response_bytes=self.config.max_response_bytes,
                             request_interval_seconds=self.config.request_interval_seconds,
                             authentication=authenticate)

    def start_recovery(self, task: dict[str, Any], source: dict[str, Any], event_id: int,
                       issues: list[dict[str, Any]], context: dict[str, Any] | None = None) -> None:
        case = {"issues": issues, "source_event_ids": [source["event_id"]], "context": context or {}}
        self.store.update_event(event_id, "failed", details={"issues": issues},
                                checkpoint={"recovery_case": case})
        self.recover_task(self.store.get_task(task["task_id"]), source)

    def recover_task(self, task: dict[str, Any], source: dict[str, Any]) -> None:
        saved = task["checkpoint"]["recovery_case"]
        case = RecoveryCase(task["task_id"], tuple(saved["issues"]),
                            tuple(saved["source_event_ids"]), saved["context"])
        tools = self.evidence_tools(task)

        async def recover():
            if self.recovery_decide is not None:
                return await run_recovery(self.store, case, policy=self.config.recovery_policy,
                                          decide=self.recovery_decide, execute=tools)
            async with RecoveryClient(self.config.recovery_endpoint, self.recovery_access_token,
                                      max_request_bytes=self.config.recovery_policy.max_input_bytes,
                                      max_response_bytes=self.config.recovery_policy.max_output_bytes,
                                      transport=self.recovery_transport) as client:
                return await run_recovery(self.store, case, policy=self.config.recovery_policy,
                                          decide=client, execute=tools)

        result = asyncio.run(recover())
        self.refresh_request_deadline()
        event = self.store.start_event(task["task_id"], "extract", details={
            "source_event_id": source["event_id"], "recovery_event_ids": list(result.event_ids),
            "recovery_outcome": result.status, "recovery_reason": result.reason})
        resolved = {}
        rejection = None
        if result.status == "proposal":
            try:
                resolved = tools.resolve(result)
            except (ValueError, KeyError, IndexError, TypeError):
                rejection = {"code": "recovery_candidate_rejected"}
        if task["kind"] == "discovery":
            try:
                if rejection or not resolved:
                    raise DiscoveryError("No validated discovery proposal")
                if task["request"]["method"] == "GET" and set(resolved) == {"category_config"}:
                    value, evidence, locator = resolved["category_config"]
                    config = parse_category_config(evidence["body"], task["request"]["url"], source_config=value)
                    self.category_configs[config.category_url] = config
                    self.store.commit_discovery(event, (config.task(),), checkpoint={
                        "category_url": config.category_url, "source_event_id": evidence["event_id"],
                        "recovered_config_locator": locator})
                elif task["request"]["method"] == "POST" and set(resolved) == {"category_page"}:
                    value, evidence, _ = resolved["category_page"]
                    config = self.category_config(task["request"]["category_url"])
                    page = decode_page(value, config, task["request"]["page"])
                    self.store.commit_discovery(event, page.targets, checkpoint=page.checkpoint)
                else:
                    raise DiscoveryError("Wrong discovery proposal kind")
                if saved["context"].get("aggregate"):
                    category = task["request"].get("category_url", task["request"]["url"])
                    report = next(item for item in self.discovery_report(self.store.list_tasks(self.run_id))
                                  if item["category_url"] == category)
                    if report["issues"]:
                        self.store.set_task_state(task["task_id"], "deferred", stage="recovery",
                            issues=[{"code": code} for code in report["issues"]])
                return
            except (DiscoveryError, ValueError, TypeError):
                issues = list(saved["issues"]) + ([rejection] if rejection else [])
                self.store.update_event(event, "failed", details={"issues": issues})
                self.store.set_task_state(task["task_id"], "deferred", stage="recovery", issues=issues)
                return
        preliminary = None
        try:
            preliminary = extract_product(source["body"], task["target_key"], source["details"]["captured_at"])
        except ExtractionError:
            pass
        if resolved:
            try:
                if resolved.keys() - {"items", "product_total"}:
                    raise ValueError("Wrong product proposal kind")
                chosen = resolved.get("items")
                total = resolved.get("product_total")
                evidence = chosen[1] if chosen else source
                candidate = extract_product(evidence["body"], task["target_key"], evidence["details"]["captured_at"],
                    source_rows=chosen[0] if chosen else None, source_total=total[0] if total else None)
                if preliminary and not {item["sku"] for item in preliminary.items} <= {item["sku"] for item in candidate.items}:
                    raise ValueError("Recovery cannot discard previously decoded SKUs")
                if chosen:
                    for fields in candidate.field_evidence.values():
                        for facts in fields.values():
                            for fact in facts:
                                if fact["source_field"].startswith("masterData."):
                                    fact["source_field"] = chosen[2] + "/" + fact["source_field"].removeprefix("masterData.")
                preliminary = candidate
            except (ValueError, TypeError):
                rejection = {"code": "recovery_candidate_rejected"}
        if preliminary is None:
            self.store.fail_product(event, expected_item_count=reported_item_count(source["body"]),
                issues=list(saved["issues"]) + ([rejection] if rejection else []))
            return
        if rejection:
            preliminary = replace(preliminary, issues=preliminary.issues + (rejection,))
        final = finalize_product(preliminary, recovery_event_ids=result.event_ids)
        self.save_product(task, source, event, final)

    def save_product(self, task: dict[str, Any], source: dict[str, Any], event_id: int,
                     final: ProductExtraction) -> None:
        issues = tuple(issue | {"source_event_id": source["event_id"]} for issue in final.issues)
        details = final.details | {"issues": list(issues)}
        if final.items:
            self.store.update_event(event_id, "started", details=details)
            self.store.commit_product(event_id, final.items, status=final.status,
                                      expected_item_count=final.expected_item_count,
                                      decoded_item_count=final.decoded_item_count, issues=issues)
        else:
            self.store.fail_product(event_id, expected_item_count=final.expected_item_count,
                                    decoded_item_count=final.decoded_item_count, issues=issues,
                                    details=details, stage="recovery")

    def collect_product(self, task: dict[str, Any], source: dict[str, Any], event_id: int) -> None:
        try:
            preliminary = extract_product(source["body"], task["target_key"], source["details"]["captured_at"])
        except ExtractionError as error:
            if self.config.recovery_policy:
                self.start_recovery(task, source, event_id, [{"code": error.code}])
            else:
                self.store.fail_product(event_id, expected_item_count=reported_item_count(source["body"]),
                    status="deferred", stage="recovery",
                    details={"recovery_skipped_reason": RECOVERY_DEFERRED_REASON},
                    issues=[{"code": error.code, "source_event_id": source["event_id"],
                             "recovery_skipped_reason": RECOVERY_DEFERRED_REASON}])
                logger.warning("Recovery deferred run_id=%s task_id=%d source_event_id=%d reason=%s",
                               self.run_id, task["task_id"], source["event_id"], RECOVERY_DEFERRED_REASON)
            return
        if preliminary.needs_recovery and self.config.recovery_policy:
            self.start_recovery(task, source, event_id, list(preliminary.issues), {
                "expected_item_count": preliminary.expected_item_count,
                "decoded_item_count": preliminary.decoded_item_count})
            return
        final = finalize_product(preliminary, recovery_skipped_reason=(
            RECOVERY_DEFERRED_REASON if preliminary.needs_recovery else None))
        self.save_product(task, source, event_id, final)
        if preliminary.needs_recovery:
            logger.warning("Recovery deferred run_id=%s task_id=%d issues=%d reason=%s",
                           self.run_id, task["task_id"], len(final.issues), RECOVERY_DEFERRED_REASON)

    def progress(self) -> None:
        products = [task for task in self.store.list_tasks(self.run_id) if task["kind"] == "product"]
        counts = Counter(task["status"] for task in products)
        logger.info("Run progress run_id=%s discovered_products=%d complete=%d partial=%d failed=%d "
                    "deferred=%d pending=%d", self.run_id, len(products), counts["complete"],
                    counts["partial"], counts["failed"], counts["deferred"], counts["pending"])

    def execute(self) -> str | None:
        discovery_reconciled = False
        while True:
            tasks = self.store.list_tasks(self.run_id)
            pending = [task for task in tasks if task["kind"] == "discovery" and task["status"] == "pending"]
            if not pending:
                if self.config.recovery_policy and not discovery_reconciled:
                    discovery_reconciled = True
                    for report in self.discovery_report(tasks):
                        candidates = [task for task in tasks if task["kind"] == "discovery"
                            and task["status"] == "complete" and "recovery_case" not in task["checkpoint"]
                            and task["request"].get("category_url") == report["category_url"]]
                        if report["issues"] and candidates:
                            task = candidates[-1]
                            self.store.set_task_state(task["task_id"], "pending", stage="recovery",
                                issues=[{"code": code, "aggregate": True} for code in report["issues"]])
                    tasks = self.store.list_tasks(self.run_id)
                    pending = [task for task in tasks if task["kind"] == "discovery" and task["status"] == "pending"]
            if not pending:
                pending = [task for task in tasks if task["kind"] == "product" and task["status"] == "pending"]
                unresolved = sum(task["kind"] == "product" and task["status"] in _UNRESOLVED for task in tasks)
                if (pending and self.config.unresolved_product_limit is not None
                        and unresolved >= self.config.unresolved_product_limit):
                    logger.warning("Product scheduling stopped run_id=%s reason=unresolved_product_limit "
                                   "unresolved=%d limit=%d remaining=%d", self.run_id, unresolved,
                                   self.config.unresolved_product_limit, len(pending))
                    return "unresolved_product_limit_reached"
            if not pending:
                return None
            now = _now()
            ready = [task for task in pending if task["next_retry_at"] is None
                     or _timestamp(task["next_retry_at"]) <= now]
            if not ready:
                self.wait_until(min(_timestamp(task["next_retry_at"]) for task in pending))
                continue
            task = ready[0]
            logger.info("Task processing run_id=%s task_id=%d kind=%s stage=%s target=%s",
                        self.run_id, task["task_id"], task["kind"], task["stage"], task["target_key"])
            source = self.source_for(task)
            if source is not None:
                if task["checkpoint"].get("recovery_case"):
                    self.recover_task(self.store.get_task(task["task_id"]), source)
                    self.progress()
                    continue
                event = self.store.start_event(task["task_id"], "extract",
                                                details={"source_event_id": source["event_id"]})
                if task["kind"] == "discovery":
                    if task["stage"] == "recovery" and task["issues"] and self.config.recovery_policy:
                        self.start_recovery(task, source, event, task["issues"], {"aggregate": True})
                    else:
                        self.discover(task, source, event)
                else:
                    self.collect_product(task, source, event)
            self.progress()

    def discovery_report(self, tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        reports = []
        for url in self.config.categories:
            discovery = [task for task in tasks if task["kind"] == "discovery"
                         and (task["request"].get("category_url") == url or task["request"]["url"] == url)]
            pages = [task["checkpoint"] for task in discovery
                     if task["request"]["method"] == "POST" and "reported_hits" in task["checkpoint"]]
            totals = {page["reported_hits"] for page in pages}
            page_counts = {page["reported_pages"] for page in pages}
            objects = [value for page in pages for value in page["object_ids"]]
            urls = {value for page in pages for value in page["product_urls"]}
            associations: dict[str, set[str]] = defaultdict(set)
            associated_urls = set()
            for task in tasks:
                for origin in task["discovered_from"]:
                    if origin.get("category_url") == url:
                        associations[origin["object_id"]].add(task["target_key"])
                        associated_urls.add(task["target_key"])
            issues = []
            if not discovery or any(task["status"] != "complete" for task in discovery) or not pages:
                issues.append("discovery_incomplete")
            if len(totals) != 1 or len(page_counts) != 1:
                issues.append("unknown_or_changing_totals")
            elif (len(pages) != max(1, next(iter(page_counts)))
                  or sum(page["decoded_hits"] for page in pages) != next(iter(totals))
                  or len(set(objects)) != next(iter(totals))):
                issues.append("category_count_mismatch")
            if any(len(targets) != 1 for targets in associations.values()):
                issues.append("conflicting_object_urls")
            if urls != associated_urls or set(objects) != set(associations):
                issues.append("discovered_product_task_mismatch")
            reports.append({"category_url": url, "reported_hits": next(iter(totals)) if len(totals) == 1 else None,
                            "reported_pages": next(iter(page_counts)) if len(page_counts) == 1 else None,
                            "committed_pages": len(pages), "decoded_hits": sum(page["decoded_hits"] for page in pages),
                            "distinct_object_ids": len(set(objects)), "distinct_product_urls": len(urls),
                            "duplicate_object_ids": len(objects) - len(set(objects)),
                            "count_status": ("unknown" if len(totals) != 1 else
                                "shortfall" if len(set(objects)) < next(iter(totals)) else
                                "excess" if len(set(objects)) > next(iter(totals)) else "matched"),
                            "missing_source_hits": max(0, next(iter(totals)) - len(set(objects))) if len(totals) == 1 else None,
                            "complete": not issues, "issues": issues})
        return reports

    def report(self, stop_reason: str | None) -> dict[str, Any]:
        tasks = self.store.list_tasks(self.run_id)
        products = [task for task in tasks if task["kind"] == "product"]
        items_by_task = {task["task_id"]: self.store.list_items(task["task_id"]) for task in products}
        rows = [item for items in items_by_task.values() for item in items]
        discovery = self.discovery_report(tasks)
        unresolved = [task for task in tasks if task["status"] in _UNRESOLVED]
        remaining = [task for task in tasks if task["status"] == "pending"]
        complete = not unresolved and not remaining and all(category["complete"] for category in discovery)
        status = "complete" if complete else "partial" if rows else "failed"
        reason = stop_reason or (None if complete else "unresolved_work" if unresolved or remaining else "discovery_incomplete")
        events = [event for task in tasks for event in self.store.list_events(task["task_id"], include_bodies=False)]
        recoveries = {}
        for event in events:
            if event["type"] == "recovery" and "recovery" in event["details"]:
                recoveries[event["task_id"]] = event["details"]["recovery"]
        recovery_cases = []
        for task_id, state in recoveries.items():
            case = {"task_id": task_id, "outcome": state["outcome"],
                    "reason": state["reason"], "attempted": state["case_reserved"],
                    "task_outcome": next(task["status"] for task in tasks if task["task_id"] == task_id)}
            diagnostic = next((step["diagnostic"] for step in reversed(state["steps"])
                               if "diagnostic" in step), None)
            if diagnostic is not None:
                case["diagnostic"] = diagnostic
            tool_errors = [{"step_id": step["step_id"], "action": step["action"], "code": step["error_code"]}
                           for step in state["steps"] if step["kind"] == "evidence" and step["status"] == "failed"]
            if tool_errors:
                case["tool_errors"] = tool_errors
            recovery_cases.append(case)
        coverage = []
        for task in products:
            expected, decoded = task["expected_item_count"], task["decoded_item_count"]
            coverage.append({"task_id": task["task_id"], "product_url": task["target_key"],
                "expected_item_count": expected, "decoded_item_count": decoded,
                "accepted_item_count": len(items_by_task[task["task_id"]]),
                "count_status": ("unknown" if expected is None or decoded is None else
                                 "shortfall" if decoded < expected else "excess" if decoded > expected else "matched"),
                "missing_source_rows": max(0, expected - decoded) if expected is not None and decoded is not None else None})
        unresolved_report = []
        for task in unresolved:
            entry = {key: task[key] for key in ("task_id", "kind", "target_key", "status", "stage", "issues")}
            entry["notes"] = [{"sku": item["sku"], **note}
                              for item in items_by_task.get(task["task_id"], []) for note in item["notes"]]
            unresolved_report.append(entry)
        report = {"run_id": self.run_id, "status": status, "stop_reason": reason,
                  "discovery": discovery, "discovered_products": len(products),
                  "product_outcomes": dict(Counter(task["status"] for task in products)),
                  "accepted_items": len(rows), "distinct_skus": len({item["sku"] for item in rows}),
                  "item_coverage": coverage,
                  "fetch_attempts": sum(event["type"] == "fetch" for event in events),
                  "uncertain_fetches": sum(event["type"] == "fetch" and event["status"] == "uncertain" for event in events),
                  "unresolved": unresolved_report,
                  "remaining_product_urls": [task["target_key"] for task in remaining if task["kind"] == "product"],
                  "remaining_discovery_tasks": [task["target_key"] for task in remaining if task["kind"] == "discovery"],
                  "recovery": {"mode": "hosted" if self.config.recovery_policy else "deferred",
                               "reason": None if self.config.recovery_policy else RECOVERY_DEFERRED_REASON,
                               "budget": self.store.get_run(self.run_id)["budget"],
                               "outcomes": dict(Counter(state["outcome"] for state in recoveries.values())),
                               "cases": recovery_cases}}
        return report


def run_catalog(database_path: str | Path, run_id: str, config: RunConfig, *, resume: bool = False,
                recover: bool = False,
                transport: httpx.BaseTransport | None = None,
                recovery_access_token: str | None = None, recovery_decide: RecoveryCall | None = None,
                recovery_transport: httpx.AsyncBaseTransport | None = None) -> dict[str, Any]:
    """Collect all discovered products and return a JSON-safe run summary.

    Own the SQLite database and synchronous fetcher for this call. New runs seed
    category configuration tasks; resume reuses the same run, code/runtime hash,
    configuration, source evidence, attempts and budgets. Terminal tasks are never
    retried implicitly. recover=True explicitly enables recovery on a stopped
    deterministic run, audits the policy change and requeues saved unresolved
    products without resetting attempts or budgets. Collection settings stay
    fixed; pending collection then continues under the original stop limit.
    Use resume=True after an interruption. Discovery finishes before products.

    Persist fetch intent before each request, and its evidence/retry decision
    together afterward. Uncertain requests consume attempts. Waiting honors the
    saved retry deadline and emits progress while waiting. Structural failures
    enter bounded recovery when configured; usable partial records are finalized
    with notes and committed. Recovery resolves source locations, then passes raw
    source values through the original validators. Recovery resumes before another
    task operation starts. No CLI or export occurs inside this function.

    Interruptions and unexpected programmer errors leave the run resumable and
    propagate. SQLite failures propagate immediately without another database
    write. SIGKILL leaves a running run that resume can reconcile. Inject only a
    trusted single-attempt transport for offline checks/configured networking.
    recovery_access_token is optional and transient; omit it for a public demo.
    Authenticated endpoints reject missing credentials at the service boundary.
    recovery_transport injects endpoint HTTP
    transport; recovery_decide optionally replaces that adapter for offline tests.
    Raise RunSetupError for invalid invocation or incompatible saved run state;
    internal collection exceptions propagate without being reclassified.
    """
    if (not isinstance(config, RunConfig) or type(resume) is not bool or type(recover) is not bool
            or resume and recover):
        raise RunSetupError("Supply a RunConfig and select at most one of resume or recover")
    if not isinstance(run_id, str) or not run_id.strip() or run_id != run_id.strip():
        raise RunSetupError("Supply a nonempty run ID without surrounding whitespace")
    settings, code_version = config.as_dict(), _code_version()
    with Storage(database_path) as store:
        try:
            if recover:
                store.enable_recovery(run_id, settings, code_version)
            elif resume:
                store.resume_run(run_id, settings, code_version)
            else:
                store.create_run(run_id, settings, code_version, budget={"reserved_cases": 0, "consumed_cases": 0})
        except (KeyError, ValueError) as error:
            raise RunSetupError(str(error)) from None
        try:
            for url in config.categories:
                store.enqueue_task(run_id, category_task(url))
            with Fetcher(timeout_seconds=config.timeout_seconds, max_response_bytes=config.max_response_bytes,
                         transport=transport) as fetcher:
                controller = _Controller(store, fetcher, run_id, config,
                    recovery_access_token=recovery_access_token, recovery_decide=recovery_decide,
                    recovery_transport=recovery_transport)
                reason = controller.execute()
                report = controller.report(reason)
            store.finish_run(run_id, report["status"], reason=report["stop_reason"])
            logger.info("Run finished run_id=%s status=%s discovered_products=%d accepted_items=%d "
                        "unresolved_tasks=%d remaining_products=%d reason=%s", run_id, report["status"],
                        report["discovered_products"], report["accepted_items"], len(report["unresolved"]),
                        len(report["remaining_product_urls"]), report["stop_reason"])
            return report
        except sqlite3.Error as error:
            logger.error("Storage failure; stopping run_id=%s error_type=%s", run_id, type(error).__name__)
            raise
        except BaseException as error:
            reason = "process_interrupted" if isinstance(error, (KeyboardInterrupt, SystemExit)) else "execution_error"
            store.finish_run(run_id, "interrupted", reason=reason)
            logger.error("Run interrupted run_id=%s reason=%s error_type=%s location=%s", run_id, reason,
                         type(error).__name__, failure_location(error))
            raise
