"""Coordinate bounded recovery; model transport, evidence tools and acceptance are external."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import re
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import asdict, dataclass, field
from time import monotonic
from typing import Any

from .diagnostics import failure_diagnostic, failure_location
from .fetching import validate_request
from .storage import Storage

logger = logging.getLogger(__name__)
SERVICE_VERSION = "safco-recovery-2"
PROMPT_VERSION = "saved-evidence-2"

RecoveryCall = Callable[[dict[str, Any], float], Awaitable[Mapping[str, Any]]]
_OUTCOMES = {"proposal", "unresolved", "budget_exhausted", "uncertain"}
_BUDGET_FIELDS = ("reserved_cases", "consumed_cases", "reserved_model_turns",
                  "completed_model_turns", "input_tokens", "output_tokens")


class _PayloadLimit(ValueError):
    pass


@dataclass(frozen=True)
class RecoveryPolicy:
    """Non-secret runtime identity and local allowances, saved in run.config.recovery.

    Reserved cases/turns count all starts, including uncertain calls; they are
    never refunded. max_case_seconds covers active execution, excluding downtime.
    Timeouts cancel cooperative async adapters; adapters must not suppress that
    cancellation or perform hidden retries. These are local execution limits,
    not authentication, hosted quotas or a guarantee about provider charges.
    """

    service_version: str
    model: str
    prompt_version: str
    max_cases: int = 3
    max_model_turns_per_run: int = 12
    max_model_turns_per_case: int = 4
    max_http_attempts_per_case: int = 3
    max_browser_actions_per_case: int = 6
    max_case_seconds: float = 180.0
    max_call_seconds: float = 60.0
    max_input_bytes: int = 64 * 1024
    max_output_bytes: int = 64 * 1024
    max_observation_bytes: int = 64 * 1024
    browser_enabled: bool = False

    def __post_init__(self) -> None:
        for name in ("service_version", "model", "prompt_version"):
            _text(getattr(self, name), name, 200)
        for name in ("max_cases", "max_model_turns_per_run", "max_model_turns_per_case",
                     "max_http_attempts_per_case", "max_browser_actions_per_case"):
            _integer(getattr(self, name), name)
        for name in ("max_input_bytes", "max_output_bytes", "max_observation_bytes"):
            _integer(getattr(self, name), name, minimum=1)
        for name in ("max_case_seconds", "max_call_seconds"):
            value = getattr(self, name)
            if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                raise ValueError(name + " must be finite and positive")
        if type(self.browser_enabled) is not bool:
            raise ValueError("browser_enabled must be boolean")

    def as_dict(self) -> dict[str, Any]:
        """Return the complete policy required at run.config.recovery."""
        return {"mode": "hosted", "protocol_version": 1, **asdict(self)}


@dataclass(frozen=True)
class RecoveryCase:
    """One task's eligible issues and original fetch sources, without credentials.

    source_event_ids must identify complete, intact successful fetches on this
    task. context is bounded JSON-safe extraction context, not final notes.
    Callers select eligible issues; confirmed absence/notes alone are not cases.
    The same inputs identify the case throughout resumption.
    """

    task_id: int
    issues: tuple[dict[str, Any], ...]
    source_event_ids: tuple[int, ...]
    context: dict[str, Any] = field(default_factory=dict, repr=False)


@dataclass(frozen=True)
class RecoveryResult:
    """Durable recovery outcome; proposal candidates still require source validation.

    event_ids includes continuation events after a restart. A proposal does not
    establish completeness or permit bypassing extraction validation. This
    module never accepts catalog rows or marks collection tasks complete.
    """

    status: str
    event_ids: tuple[int, ...]
    reason: str | None
    candidates: tuple[dict[str, Any], ...] = field(default=(), repr=False)


class RecoveryActionError(Exception):
    """An adapter's expected failure, with a safe code and explicit uncertainty."""

    def __init__(self, code: str, *, uncertain: bool = True) -> None:
        """Use uncertain=False only when the adapter knows the action's outcome."""
        if not isinstance(code, str) or re.fullmatch(r"[a-z][a-z0-9_]{0,63}", code) is None:
            raise ValueError("Supply a stable error code without response text or credentials")
        if type(uncertain) is not bool:
            raise ValueError("uncertain must be boolean")
        self.code, self.uncertain = code, uncertain
        super().__init__(code)


def _integer(value: Any, name: str, *, minimum: int = 0) -> None:
    if type(value) is not int or value < minimum:
        raise ValueError(name + " must be an integer >= " + str(minimum))


def _text(value: Any, name: str, limit: int = 2000) -> None:
    if (not isinstance(value, str) or not value.strip() or value != value.strip()
            or len(value) > limit or any(ord(char) < 32 or ord(char) == 127 for char in value)):
        raise ValueError("Invalid " + name)


def _copy(value: Any, limit: int | None = None) -> Any:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"),
                         ensure_ascii=False, allow_nan=False).encode()
    if limit is not None and len(encoded) > limit:
        raise _PayloadLimit("JSON byte allowance exceeded")
    return json.loads(encoded)


def _sources(store: Storage, task_id: int) -> list[dict[str, Any]]:
    sources = []
    for event in store.list_events(task_id):
        if event["type"] != "fetch" or event["status"] != "succeeded":
            continue
        details, body = event["details"], event["body"]
        status = details.get("http_status")
        if (type(status) is not int or not 200 <= status < 300
                or details.get("body_complete") is not True or body is None):
            continue
        if hashlib.sha256(body).hexdigest() != event["content_hash"]:
            raise ValueError("Recovery source checksum mismatch")
        sources.append({"source_event_id": event["event_id"],
                        "evidence_id": "source:" + str(event["event_id"]),
                        "content_hash": event["content_hash"], "saved_bytes": len(body)})
    return sources


def _request_scope(request: Any) -> None:
    validate_request(request)


def _decision(value: Any, sources: list[dict[str, Any]], steps: list[dict[str, Any]],
              policy: RecoveryPolicy) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != {"action", "arguments"}:
        raise ValueError("A decision requires action and arguments")
    action, args = value["action"], value["arguments"]
    if not isinstance(action, str) or not isinstance(args, dict):
        raise ValueError("Invalid decision types")
    if action == "read_source":
        if not {"source_event_id", "view"} <= args.keys() or args.keys() - {
                "source_event_id", "view", "locator", "offset", "limit_bytes"}:
            raise ValueError("Invalid source-reader arguments")
        _integer(args["source_event_id"], "source_event_id", minimum=1)
        if (args["source_event_id"] not in {source["source_event_id"] for source in sources}
                or args["view"] not in ("scripts", "json", "tables", "text")):
            raise ValueError("Unknown source or view")
        if "locator" in args:
            _text(args["locator"], "source locator")
        _integer(args.get("offset", 0), "offset")
        _integer(args.get("limit_bytes", policy.max_observation_bytes), "limit_bytes", minimum=1)
        if args.get("limit_bytes", 0) > policy.max_observation_bytes:
            raise ValueError("Source slice exceeds the evidence allowance")
    elif action == "fetch_evidence":
        if set(args) != {"request"}:
            raise ValueError("Fetch evidence requires a request")
        _request_scope(args["request"])
    elif action == "browser_evidence":
        _text(args.get("reason"), "browser escalation reason", 1000)
        fields = set(args) - {"reason"}
        operation = args.get("operation")
        if operation == "open" and fields == {"operation", "url"}:
            _request_scope({"method": "GET", "url": args["url"]})
        elif operation == "inspect" and fields == {"operation"}:
            pass
        elif operation == "click_next" and fields == {"operation", "element_ref"}:
            _text(args["element_ref"], "pagination element reference")
        else:
            raise ValueError("Unsupported browser operation")
    elif action == "submit_candidates":
        if set(args) != {"candidates"} or not isinstance(args["candidates"], list) or not args["candidates"]:
            raise ValueError("Submit a nonempty candidate list")
        known = {source["evidence_id"] for source in sources}
        known.update(step["evidence_id"] for step in steps
                     if step["kind"] == "evidence" and step["status"] == "succeeded")
        for candidate in args["candidates"]:
            if (not isinstance(candidate, dict) or set(candidate) != {"record", "evidence"}
                    or not isinstance(candidate["record"], dict) or not candidate["record"]
                    or not isinstance(candidate["evidence"], list) or not candidate["evidence"]):
                raise ValueError("Each candidate requires a record and evidence references")
            for reference in candidate["evidence"]:
                if not isinstance(reference, dict) or set(reference) != {"evidence_id", "locator"}:
                    raise ValueError("Invalid candidate evidence reference")
                _text(reference["evidence_id"], "evidence reference")
                _text(reference["locator"], "evidence locator")
                if reference["evidence_id"] not in known:
                    raise ValueError("Candidate cites unknown evidence")
    elif action == "finish_unresolved":
        if set(args) != {"reason"}:
            raise ValueError("Unresolved decisions require a reason")
        _text(args["reason"], "unresolved reason", 1000)
    else:
        raise ValueError("Unsupported recovery action")
    return value


class _Recovery:
    def __init__(self, store: Storage, case: RecoveryCase, policy: RecoveryPolicy,
                 decide: RecoveryCall, execute: RecoveryCall):
        self.store, self.policy, self.decide, self.execute = store, policy, decide, execute
        _integer(case.task_id, "task_id", minimum=1)
        self.task = store.get_task(case.task_id)
        self.run_id = self.task["run_id"]
        run = store.get_run(self.run_id)
        if run["status"] != "running" or run["config"].get("recovery") != policy.as_dict():
            raise ValueError("Recovery requires a running run with the original saved recovery policy")
        if (not isinstance(case.issues, (tuple, list)) or not case.issues
                or any(not isinstance(issue, Mapping) or not isinstance(issue.get("code"), str)
                       or not issue["code"].strip() for issue in case.issues)):
            raise ValueError("Supply eligible issue objects with nonempty codes")
        if not isinstance(case.source_event_ids, (tuple, list)) or not case.source_event_ids:
            raise ValueError("Supply original source event IDs")
        for source_id in case.source_event_ids:
            _integer(source_id, "source_event_id", minimum=1)
        if len(set(case.source_event_ids)) != len(case.source_event_ids) or not isinstance(case.context, Mapping):
            raise ValueError("Source IDs must be distinct and context must be an object")
        sources = {source["source_event_id"]: source for source in _sources(store, case.task_id)}
        if any(source_id not in sources for source_id in case.source_event_ids):
            raise ValueError("Case sources must be complete, intact fetches on this task")
        self.case = _copy({"task_id": case.task_id, "kind": self.task["kind"],
                           "target_key": self.task["target_key"], "issues": list(case.issues),
                           "sources": [sources[key] for key in sorted(case.source_event_ids)],
                           "context": dict(case.context)}, policy.max_input_bytes)
        self.case_hash = hashlib.sha256(json.dumps(self.case, sort_keys=True).encode()).hexdigest()
        managed = [event for event in store.list_events(case.task_id)
                   if event["type"] == "recovery" and "recovery" in event["details"]]
        self.event_ids = [event["event_id"] for event in managed]
        self.previous = managed[-1] if managed else None
        if any(event["details"]["recovery"].get("case_hash") != self.case_hash for event in managed):
            raise ValueError("This task already has a different recovery case; explicit retry policy is required")
        self.state = (_copy(self.previous["details"]["recovery"]) if self.previous else
                      {"version": 1, "case_hash": self.case_hash, "case": self.case,
                       "case_reserved": False, "steps": [], "elapsed_seconds": 0.0,
                       "outcome": None, "reason": None})
        if (self.state["version"] != 1 or self.state["case"] != self.case
                or self.state["outcome"] not in _OUTCOMES | {None}
                or type(self.state["case_reserved"]) is not bool
                or type(self.state["elapsed_seconds"]) not in (int, float)
                or not math.isfinite(self.state["elapsed_seconds"]) or self.state["elapsed_seconds"] < 0
                or not isinstance(self.state["steps"], list)):
            raise ValueError("Invalid saved recovery checkpoint")
        self.budget = dict(run["budget"])
        if any(self.budget.get(key, 0) for key in _BUDGET_FIELDS) and any(
                key not in self.budget for key in _BUDGET_FIELDS):
            raise ValueError("Incomplete nonzero recovery budget cannot be reset")
        for key in _BUDGET_FIELDS:
            self.budget.setdefault(key, 0)
            _integer(self.budget[key], key)
        if (self.budget["consumed_cases"] > self.budget["reserved_cases"]
                or self.budget["completed_model_turns"] > self.budget["reserved_model_turns"]):
            raise ValueError("Invalid saved recovery budget")
        self.event_id = self.previous["event_id"] if self.previous else None
        self.started, self.elapsed_before = monotonic(), self.state["elapsed_seconds"]

    def elapsed(self) -> float:
        return self.elapsed_before + monotonic() - self.started

    def save(self, status: str = "started") -> None:
        checkpoint = {"recovery_event_id": self.event_id}
        self.store.update_event(self.event_id, status, details={"recovery": self.state},
                                checkpoint=checkpoint, budget=self.budget)

    def result(self) -> RecoveryResult:
        candidates = ()
        if self.state["outcome"] == "proposal":
            candidates = tuple(_copy(self.state["steps"][-1]["result"]["decision"]["arguments"]["candidates"]))
        return RecoveryResult(self.state["outcome"], tuple(self.event_ids), self.state["reason"], candidates)

    def finish(self, outcome: str, reason: str | None = None) -> RecoveryResult:
        self.state.update(outcome=outcome, reason=reason)
        if self.state["case_reserved"] and outcome != "uncertain":
            self.budget["consumed_cases"] += 1
        self.save("succeeded" if outcome == "proposal" else "uncertain" if outcome == "uncertain" else "failed")
        log = logger.info if outcome == "proposal" else logger.warning
        log("Recovery outcome run_id=%s task_id=%d event_id=%d outcome=%s reason=%s "
            "model_turns=%d elapsed_seconds=%.3f", self.run_id, self.task["task_id"],
            self.event_id, outcome, reason, self.count("model"), self.state["elapsed_seconds"])
        return self.result()

    def count(self, kind: str, action: str | None = None) -> int:
        return sum(step["kind"] == kind and (action is None or step.get("action") == action)
                   for step in self.state["steps"])

    def begin(self) -> RecoveryResult | None:
        if self.state["outcome"] is not None:
            logger.info("Reusing recovery outcome task_id=%d event_id=%d", self.task["task_id"], self.event_id)
            return self.result()
        if self.task["status"] not in {"pending", "running"}:
            raise ValueError("Recovery requires pending/running work or an existing saved outcome")
        if self.previous is not None:
            pending = [step for step in self.state["steps"] if step["status"] in {"started", "uncertain"}]
            if pending:
                for step in pending:
                    step.update(status="uncertain", error_code="outcome_not_saved")
                return self.finish("uncertain", "outcome_not_saved")
            # Starting a resumed pending task changes its attempt; close the old
            # operation first so Storage never updates an older attempt afterward.
            if self.previous["status"] in {"started", "uncertain"}:
                self.save("succeeded")
            self.state["continued_from_event_id"] = self.previous["event_id"]
        reason = None
        if self.previous is None:
            if self.budget["reserved_cases"] >= self.policy.max_cases:
                reason = "case_limit"
            elif self.budget["reserved_model_turns"] >= self.policy.max_model_turns_per_run:
                reason = "run_model_turn_limit"
            elif self.policy.max_model_turns_per_case == 0:
                reason = "case_model_turn_limit"
            else:
                self.state["case_reserved"] = True
                self.budget["reserved_cases"] += 1
        self.event_id = self.store.start_event(self.task["task_id"], "recovery",
                                              details={"recovery": self.state}, budget=self.budget)
        self.event_ids.append(self.event_id)
        self.save()
        if reason is not None:
            logger.warning("Recovery not attempted run_id=%s task_id=%d event_id=%d reason=%s",
                           self.run_id, self.task["task_id"], self.event_id, reason)
            return self.finish("budget_exhausted", reason)
        logger.info("Recovery started run_id=%s task_id=%d event_id=%d resumed=%s "
                    "remaining_cases=%d remaining_model_turns=%d", self.run_id, self.task["task_id"],
                    self.event_id, self.previous is not None,
                    max(0, self.policy.max_cases - self.budget["reserved_cases"]),
                    max(0, self.policy.max_model_turns_per_run - self.budget["reserved_model_turns"]))
        return None

    def model_request(self, step_id: str) -> dict[str, Any]:
        history = []
        for step in self.state["steps"]:
            if step["kind"] == "model":
                history.append({"step_id": step["step_id"], "decision": step["result"]["decision"]})
            else:
                history.append({"step_id": step["step_id"], "action": step["action"],
                                "arguments": step["input"]["arguments"], "evidence_id": step["evidence_id"],
                                "observation": step["result"]})
        actions = {"read_source", "submit_candidates", "finish_unresolved"}
        if self.count("evidence", "fetch_evidence") < self.policy.max_http_attempts_per_case:
            actions.add("fetch_evidence")
        if self.policy.browser_enabled and self.count("evidence", "browser_evidence") < self.policy.max_browser_actions_per_case:
            actions.add("browser_evidence")
        return _copy({"protocol_version": 1, "request_id": step_id, "case": self.case,
                      "task_request": self.task["request"],
                      "remaining_model_turns": min(self.policy.max_model_turns_per_run - self.budget["reserved_model_turns"],
                                                   self.policy.max_model_turns_per_case - self.count("model")),
                      "expected": {name: getattr(self.policy, name)
                                   for name in ("service_version", "model", "prompt_version")},
                      "sources": _sources(self.store, self.task["task_id"]), "history": history,
                      "available_actions": sorted(actions)},
                     self.policy.max_input_bytes)

    def model_result(self, value: Any, request_id: str) -> dict[str, Any]:
        value = _copy(value, self.policy.max_output_bytes)
        required = {"request_id", "service_version", "model", "prompt_version", "decision", "usage"}
        if not isinstance(value, dict) or not required <= value.keys() or value.keys() - required - {"provider_request_id"}:
            raise ValueError("Invalid hosted response envelope")
        if value["request_id"] != request_id or any(
                value[name] != getattr(self.policy, name) for name in ("service_version", "model", "prompt_version")):
            raise ValueError("Hosted response identity differs from the saved policy/request")
        if "provider_request_id" in value:
            _text(value["provider_request_id"], "provider request ID", 500)
        self.usage(value)
        value["decision"] = _decision(value["decision"], _sources(self.store, self.task["task_id"]),
                                       self.state["steps"], self.policy)
        return value

    def usage(self, value: Any) -> dict[str, int]:
        usage = value.get("usage") if isinstance(value, Mapping) else None
        if not isinstance(usage, dict) or set(usage) != {"input_tokens", "output_tokens"}:
            raise ValueError("Invalid model usage")
        for name in ("input_tokens", "output_tokens"):
            _integer(usage[name], name)
        return dict(usage)

    async def call(self, kind: str, payload: dict[str, Any], action: str | None = None) -> RecoveryResult | None:
        remaining = self.policy.max_case_seconds - self.elapsed()
        if remaining <= 0:
            self.state["elapsed_seconds"] = self.elapsed()
            return self.finish("budget_exhausted", "case_time_limit")
        timeout = min(self.policy.max_call_seconds, remaining)
        step_id = self.case_hash + ":" + str(len(self.state["steps"]) + 1)
        step = {"step_id": step_id, "kind": kind, "status": "started", "input": payload,
                "timeout_seconds": timeout}
        if kind == "model":
            self.budget["reserved_model_turns"] += 1
        else:
            step.update(action=action, evidence_id=payload["evidence_id"])
        self.state["steps"].append(step)
        self.state["elapsed_seconds"] = self.elapsed() + timeout
        self.save()
        logger.info("Recovery action task_id=%d event_id=%d step=%d kind=%s action=%s timeout_seconds=%.3f",
                    self.task["task_id"], self.event_id, len(self.state["steps"]), kind, action, timeout)
        try:
            async with asyncio.timeout(timeout):
                raw = await (self.decide if kind == "model" else self.execute)(_copy(payload), timeout)
        except (TimeoutError, RecoveryActionError) as error:
            uncertain = isinstance(error, TimeoutError) or error.uncertain
            step.update(status="uncertain" if uncertain else "failed",
                        error_code="action_timeout" if isinstance(error, TimeoutError) else error.code)
            if (kind == "evidence" and action == "read_source" and not uncertain
                    and error.code == "evidence_location_invalid"):
                step["result"] = {"error": {"code": error.code}}
                self.state["elapsed_seconds"] = self.elapsed()
                self.save()
                logger.warning("Recovery source lookup rejected task_id=%d event_id=%d code=%s feedback_saved=true",
                               self.task["task_id"], self.event_id, error.code)
                return None
            if not uncertain:
                self.state["elapsed_seconds"] = self.elapsed()
            else:
                self.state["elapsed_seconds"] = max(self.state["elapsed_seconds"], self.elapsed())
            return self.finish("uncertain" if uncertain else "unresolved", step["error_code"])
        if kind == "model":
            self.budget["completed_model_turns"] += 1
            try:
                step["usage"] = self.usage(raw)
            except ValueError:
                step["usage"] = None
            if step["usage"] is not None:
                for name, value in step["usage"].items():
                    self.budget[name] += value
        try:
            if kind == "model":
                step["result"] = self.model_result(raw, step_id)
            else:
                if not isinstance(raw, Mapping):
                    raise ValueError("Evidence observations must be JSON objects")
                step["result"] = _copy(dict(raw), self.policy.max_observation_bytes)
        except _PayloadLimit:
            step.update(status="failed", error_code="model_output_limit" if kind == "model" else "observation_byte_limit")
            self.state["elapsed_seconds"] = self.elapsed()
            return self.finish("budget_exhausted", step["error_code"])
        except (ValueError, TypeError, RecursionError, OverflowError) as error:
            step.update(status="failed", error_code="invalid_model_response" if kind == "model" else "invalid_observation")
            step["diagnostic"] = failure_diagnostic(error)
            if kind == "model" and isinstance(raw, Mapping) and "decision" in raw:
                try:
                    step["rejected_decision"] = _copy(raw["decision"], self.policy.max_output_bytes)
                except (ValueError, TypeError, RecursionError, OverflowError):
                    pass
            logger.warning("Recovery validation failed task_id=%d event_id=%d code=%s location=%s",
                           self.task["task_id"], self.event_id, step["error_code"], failure_location(error))
            self.state["elapsed_seconds"] = self.elapsed()
            return self.finish("unresolved", step["error_code"])
        step["status"] = "succeeded"
        self.state["elapsed_seconds"] = self.elapsed()
        self.save()
        logger.info("Recovery observation task_id=%d event_id=%d step=%d kind=%s saved=true",
                    self.task["task_id"], self.event_id, len(self.state["steps"]), kind)
        return None

    async def run(self) -> RecoveryResult:
        result = self.begin()
        if result is not None:
            return result
        while True:
            if self.elapsed() >= self.policy.max_case_seconds:
                self.state["elapsed_seconds"] = self.elapsed()
                return self.finish("budget_exhausted", "case_time_limit")
            last = self.state["steps"][-1] if self.state["steps"] else None
            if last is not None and last["kind"] == "model":
                decision = last["result"]["decision"]
                action = decision["action"]
                if action == "submit_candidates":
                    return self.finish("proposal")
                if action == "finish_unresolved":
                    return self.finish("unresolved", "agent_unresolved")
                if action == "fetch_evidence" and self.count("evidence", action) >= self.policy.max_http_attempts_per_case:
                    return self.finish("budget_exhausted", "http_attempt_limit")
                if action == "browser_evidence":
                    if not self.policy.browser_enabled:
                        return self.finish("unresolved", "browser_disabled")
                    if self.count("evidence", action) >= self.policy.max_browser_actions_per_case:
                        return self.finish("budget_exhausted", "browser_action_limit")
                result = await self.call("evidence", {
                    "task_id": self.task["task_id"], "action": action, "arguments": decision["arguments"],
                    "evidence_id": "observation:" + str(len(self.state["steps"]) + 1)}, action)
            else:
                if self.budget["reserved_model_turns"] >= self.policy.max_model_turns_per_run:
                    return self.finish("budget_exhausted", "run_model_turn_limit")
                if self.count("model") >= self.policy.max_model_turns_per_case:
                    return self.finish("budget_exhausted", "case_model_turn_limit")
                try:
                    payload = self.model_request(self.case_hash + ":" + str(len(self.state["steps"]) + 1))
                except _PayloadLimit:
                    return self.finish("budget_exhausted", "model_input_limit")
                result = await self.call("model", payload)
            if result is not None:
                return result


async def run_recovery(store: Storage, case: RecoveryCase, *, policy: RecoveryPolicy,
                       decide: RecoveryCall, execute: RecoveryCall) -> RecoveryResult:
    """Recover one eligible task using injected async model and evidence calls.

    The caller owns Storage on this coroutine's thread and saves policy.as_dict()
    in run.config.recovery before collection. Each adapter accepts (request,
    timeout_seconds), performs exactly one bounded action and returns a JSON
    object. No retries, detached work, budget changes or task finalization/requeue
    are permitted in adapters. Evidence adapters may append fetch events to the
    current attempt without promoting them to the task's primary source.
    They enforce source/URL/element scope, reject redirects outside it and preserve
    full HTTP responses as fetch evidence when applicable.

    Model requests contain case/source metadata, prior decisions/observations,
    expected service/model/prompt versions, the saved task request, the remaining
    model turns including this call, and a stable request_id. Responses
    must echo those identities and contain decision={action, arguments} and
    usage={input_tokens, output_tokens}; provider_request_id is optional. Evidence
    observations receive short, case-scoped IDs such as observation:4; step IDs
    remain globally correlatable. Known local location errors are saved and fed
    back within the existing budget. Browser actions require an explicit
    escalation reason and browser_enabled=True. Candidate records require evidence
    references with evidence_id and locator; source-value validation is still
    mandatory after this function returns a proposal.

    Reservations and step intent are saved atomically before calls. Completed
    observations are replayed after restart; an outstanding intent produces an
    uncertain result without resubmission. Recovery continuation uses a new
    operation on a resumed task, retaining the same case and all allowances.
    Call Storage.resume_run before continuing an interrupted run, then call this
    function before starting other operations on the resumed task. Terminal tasks
    require explicit requeueing, except read-only replay in a running run.

    Expected adapter failures use RecoveryActionError. Cancellation, unexpected
    programmer errors and SQLite errors propagate without further writes, leaving
    saved intent for resumption. No credentials, provider SDKs, network clients,
    browser instances, catalog acceptance or runner wiring are created here.
    """
    if not isinstance(case, RecoveryCase) or not isinstance(policy, RecoveryPolicy):
        raise ValueError("Supply a RecoveryCase and RecoveryPolicy")
    if not callable(decide) or not callable(execute):
        raise ValueError("Supply async model and evidence callables")
    return await _Recovery(store, case, policy, decide, execute).run()
