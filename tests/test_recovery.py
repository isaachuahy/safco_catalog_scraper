"""Recovery contracts checked offline against real SQLite and abrupt process exits."""

import asyncio
import copy
import json
import logging
import os
import signal
import sqlite3
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

import pytest

from safco_catalog_scraper import recovery
from safco_catalog_scraper.recovery import (
    RecoveryActionError, RecoveryCase, RecoveryPolicy, run_recovery,
)
from safco_catalog_scraper.storage import Storage, TaskSpec

POLICY = RecoveryPolicy("fixture-service", "fixture-model", "fixture-prompt")
CODE_VERSION = "recovery-fixture"
SOURCE = {"alternate": [{"sku": "0001", "price": "16.49"}]}


def _proposal(evidence_id):
    return {"action": "submit_candidates", "arguments": {"candidates": [
        {"record": {"sku": "0001", "base_price": "16.49"},
         "evidence": [{"evidence_id": evidence_id, "locator": "/alternate/0"}]},
    ]}}


def _reply(request, decision):
    return {"request_id": request["request_id"], **request["expected"],
            "decision": decision, "usage": {"input_tokens": 10, "output_tokens": 5}}


class _Harness:
    def __init__(self, path, policy=POLICY):
        self.path, self.policy = path, policy
        self.config = {"recovery": policy.as_dict()}
        self.store = Storage(path)
        self.store.create_run("run", self.config, CODE_VERSION)
        self.cases, self.model_calls, self.tool_calls = [], [], []
        self.decision = None
        self.transform_reply = None
        self.observation = copy.deepcopy(SOURCE)
        self.case = self.add_case()

    def add_case(self, kind="product"):
        prefix = "product/fixture-" if kind == "product" else "catalog/gloves?fixture="
        url = "https://www.safcodental.com/" + prefix + str(len(self.cases))
        task_id = self.store.enqueue_task("run", TaskSpec(kind, url, {"method": "GET", "url": url}))
        source_id = self.store.start_event(task_id, "fetch")
        self.store.update_event(source_id, "succeeded", body=json.dumps(SOURCE).encode(),
                                details={"http_status": 200, "body_complete": True},
                                checkpoint={"source_event_id": source_id})
        code = "missing_masterData" if kind == "product" else "discovery_structure"
        case = RecoveryCase(task_id, ({"code": code},), (source_id,))
        self.cases.append(case)
        return case

    @property
    def budget(self):
        return self.store.get_run("run")["budget"]

    def events(self, case=None):
        return [event for event in self.store.list_events((case or self.case).task_id)
                if event["type"] == "recovery"]

    async def decide(self, request, timeout):
        self.model_calls.append(copy.deepcopy(request))
        if self.decision is not None:
            decision = self.decision(request)
        elif not request["history"]:
            decision = {"action": "read_source", "arguments": {
                "source_event_id": request["case"]["sources"][0]["source_event_id"], "view": "json"}}
        else:
            decision = _proposal(request["history"][-1]["evidence_id"])
        response = _reply(request, decision)
        if self.transform_reply is not None:
            self.transform_reply(response)
        return response

    async def execute(self, request, timeout):
        self.tool_calls.append(copy.deepcopy(request))
        return copy.deepcopy(self.observation)

    def run(self, case=None, *, decide=None, execute=None):
        return asyncio.run(run_recovery(self.store, case or self.case, policy=self.policy,
                                        decide=decide or self.decide, execute=execute or self.execute))

    def resume(self):
        self.store.close()
        self.store = Storage(self.path)
        self.store.resume_run("run", self.config, CODE_VERSION)

    def close(self):
        self.store.close()


@pytest.fixture
def build(tmp_path):
    """Create isolated runs with configurable saved policies and scripted adapters."""
    harnesses = []

    def create(policy=POLICY):
        harness = _Harness(tmp_path / (str(len(harnesses)) + ".sqlite"), policy)
        harnesses.append(harness)
        return harness

    yield create
    for harness in harnesses:
        harness.close()


@pytest.fixture
def harness(build):
    """Provide an unfinished product task with complete saved source evidence."""
    return build()


class _Interrupted(BaseException):
    pass


def _interrupt():
    raise _Interrupted()


def _interrupt_on_save(store, phase, interrupt=_interrupt):
    original = store.update_event
    fired = False

    def update(event_id, status, **kwargs):
        nonlocal fired
        state = kwargs.get("details", {}).get("recovery", {})
        steps = state.get("steps", [])
        last = steps[-1] if steps else {}
        match = not fired and (
            (phase == "model_intent" and last.get("kind") == "model" and last.get("status") == "started")
            or (phase in {"model_observation", "before_observation_save"} and len(steps) == 1
                and last.get("status") == "succeeded")
            or (phase == "tool_observation" and last.get("kind") == "evidence"
                and last.get("status") == "succeeded")
            or (phase == "final_result" and state.get("outcome") == "proposal")
        )
        if match:
            fired = True
            if phase == "before_observation_save":
                interrupt()
        original(event_id, status, **kwargs)
        if match:
            interrupt()

    store.update_event = update


@pytest.mark.parametrize("kind", ["product", "discovery"])
def test_proposals_retain_identity_and_evidence_without_accepting_catalog_rows(harness, kind):
    case = harness.case if kind == "product" else harness.add_case(kind)
    result = harness.run(case)
    assert result.status == "proposal"
    assert result.reason is None
    assert result.candidates[0]["record"] == {"sku": "0001", "base_price": "16.49"}
    assert result.candidates[0]["evidence"][0]["evidence_id"] == harness.tool_calls[0]["evidence_id"]
    assert not harness.store.list_items(case.task_id)
    assert harness.store.get_task(case.task_id)["status"] == "running"
    event = harness.events(case)[-1]
    assert event["status"] == "succeeded"
    assert [step["kind"] for step in event["details"]["recovery"]["steps"]] == ["model", "evidence", "model"]
    assert harness.budget == {"reserved_cases": 1, "consumed_cases": 1,
                              "reserved_model_turns": 2, "completed_model_turns": 2,
                              "input_tokens": 20, "output_tokens": 10}


def test_intent_and_allowance_are_durable_before_each_adapter_call(harness):
    async def decide(request, timeout):
        state = harness.events()[-1]["details"]["recovery"]
        step = state["steps"][-1]
        assert step["status"] == "started" and step["kind"] == "model"
        assert step["input"] == request
        assert state["elapsed_seconds"] >= timeout > 0
        assert harness.budget["reserved_model_turns"] == len(harness.model_calls) + 1
        return await harness.decide(request, timeout)

    async def execute(request, timeout):
        step = harness.events()[-1]["details"]["recovery"]["steps"][-1]
        assert step["status"] == "started" and step["kind"] == "evidence"
        assert step["input"] == request
        assert step["evidence_id"] == request["evidence_id"]
        return await harness.execute(request, timeout)

    assert harness.run(decide=decide, execute=execute).status == "proposal"


def test_saved_outcome_replays_without_calls_budget_changes_or_new_events(harness):
    expected = harness.run()
    harness.resume()
    before = (harness.budget, harness.events(), harness.store.get_task(harness.case.task_id))
    assert harness.run() == expected
    assert (harness.budget, harness.events(), harness.store.get_task(harness.case.task_id)) == before
    assert len(harness.model_calls) == 2 and len(harness.tool_calls) == 1


@pytest.mark.parametrize("field,reason", [
    ("max_cases", "case_limit"),
    ("max_model_turns_per_run", "run_model_turn_limit"),
    ("max_model_turns_per_case", "case_model_turn_limit"),
])
def test_zero_allowance_records_deferral_without_external_calls(build, field, reason):
    harness = build(replace(POLICY, **{field: 0}))
    result = harness.run()
    assert (result.status, result.reason) == ("budget_exhausted", reason)
    assert not harness.model_calls and not harness.tool_calls
    assert harness.budget["reserved_cases"] == harness.budget["reserved_model_turns"] == 0
    assert harness.events()[-1]["details"]["recovery"]["outcome"] == "budget_exhausted"


@pytest.mark.parametrize("field,limit,reason", [
    ("max_cases", 1, "case_limit"),
    ("max_model_turns_per_run", 2, "run_model_turn_limit"),
])
def test_run_allowance_is_shared_across_product_tasks(build, field, limit, reason):
    harness = build(replace(POLICY, **{field: limit}))
    assert harness.run().status == "proposal"
    before = harness.budget
    second = harness.add_case()
    assert harness.run(second).reason == reason
    assert harness.budget == before
    assert len(harness.model_calls) == 2 and len(harness.tool_calls) == 1


def test_model_turn_limit_stops_a_case_after_saved_evidence(build):
    harness = build(replace(POLICY, max_model_turns_per_case=1))
    result = harness.run()
    assert (result.status, result.reason) == ("budget_exhausted", "case_model_turn_limit")
    assert len(harness.model_calls) == len(harness.tool_calls) == 1
    assert harness.events()[-1]["details"]["recovery"]["steps"][-1]["result"] == SOURCE


@pytest.mark.parametrize("action,field,arguments,reason", [
    ("fetch_evidence", "max_http_attempts_per_case",
     {"request": {"method": "GET", "url": "https://www.safcodental.com/product/fixture"}}, "http_attempt_limit"),
    ("browser_evidence", "max_browser_actions_per_case",
     {"operation": "inspect", "reason": "Saved HTTP evidence lacks rendered SKU rows"}, "browser_action_limit"),
])
def test_evidence_action_limits_are_enforced_before_another_action(build, action, field, arguments, reason):
    harness = build(replace(POLICY, browser_enabled=True, **{field: 1}))
    harness.decision = lambda request: {"action": action, "arguments": arguments}
    result = harness.run()
    assert (result.status, result.reason) == ("budget_exhausted", reason)
    assert len(harness.model_calls) == 2 and len(harness.tool_calls) == 1


@pytest.mark.parametrize("enabled,arguments,reason", [
    (False, {"operation": "inspect", "reason": "Rendered rows are absent from the saved HTTP response"}, "browser_disabled"),
    (True, {"operation": "inspect"}, "invalid_model_response"),
    (True, {"operation": "execute_javascript", "reason": "Inspect page"}, "invalid_model_response"),
])
def test_browser_access_requires_configuration_reason_and_a_permitted_operation(build, enabled, arguments, reason):
    harness = build(replace(POLICY, browser_enabled=enabled))
    harness.decision = lambda request: {"action": "browser_evidence", "arguments": arguments}
    assert harness.run().reason == reason
    assert not harness.tool_calls
    assert ("browser_evidence" in harness.model_calls[0]["available_actions"]) == enabled


@pytest.mark.parametrize("field,value", [
    ("request_id", "another-request"), ("service_version", "another-service"),
    ("model", "another-model"), ("prompt_version", "another-prompt"),
    ("reasoning", "PRIVATE_MODEL_REASONING"),
])
def test_invalid_hosted_response_is_rejected_while_known_usage_is_retained(harness, field, value):
    harness.transform_reply = lambda response: response.update({field: value})
    result = harness.run()
    assert (result.status, result.reason) == ("unresolved", "invalid_model_response")
    assert not harness.tool_calls
    assert harness.budget["input_tokens"] == 10 and harness.budget["output_tokens"] == 5
    assert harness.budget["completed_model_turns"] == harness.budget["reserved_model_turns"] == 1
    assert "PRIVATE_MODEL_REASONING" not in json.dumps(harness.events())


@pytest.mark.parametrize("decision", [
    {"action": "submit_candidates", "arguments": {"candidates": []}},
    {"action": "submit_candidates", "arguments": {"candidates": [{"record": {"sku": "0001"}, "evidence": []}]}},
    _proposal("source:999999"),
    {"action": "read_source", "arguments": {"source_event_id": 999999, "view": "json"}},
    {"action": "run_code", "arguments": {"code": "raise AssertionError('must not execute')"}},
])
def test_missing_or_unknown_evidence_and_unsupported_actions_are_rejected(harness, decision):
    harness.decision = lambda request: decision
    assert harness.run().reason == "invalid_model_response"
    assert not harness.tool_calls and not harness.store.list_items(harness.case.task_id)


@pytest.mark.parametrize("evidence_request", [
    {"method": "GET", "url": "https://localhost/product/private"},
    {"method": "GET", "url": "http://www.safcodental.com/product/fixture"},
    {"method": "GET", "url": "https://user:secret@www.safcodental.com/product/fixture"},
    {"method": "GET", "url": "https://www.safcodental.com/product/%2e%2e/private"},
    {"method": "POST", "url": "https://www.safcodental.com/product/fixture", "json": {}},
    {"method": "GET", "url": "https://www.safcodental.com/product/fixture", "headers": {"Authorization": "secret"}},
])
def test_out_of_scope_requests_never_reach_the_evidence_adapter(harness, evidence_request):
    harness.decision = lambda model_request: {"action": "fetch_evidence", "arguments": {"request": evidence_request}}
    assert harness.run().reason == "invalid_model_response"
    assert not harness.tool_calls


@pytest.mark.parametrize("evidence_request", [
    {"method": "GET", "url": "https://www.safcodental.com/product/fixture"},
    {"method": "GET", "url": "https://www.safcodental.com/catalog/gloves"},
    {"method": "POST", "url": "https://fixture-dsn.algolia.net/1/indexes/*/queries",
     "json": {"requests": [{"indexName": "fixture", "params": "page=1"}]}},
])
def test_scoped_safco_and_algolia_requests_can_produce_evidence(harness, evidence_request):
    harness.decision = lambda model_request: (
        _proposal(model_request["history"][-1]["evidence_id"]) if model_request["history"]
        else {"action": "fetch_evidence", "arguments": {"request": evidence_request}})
    assert harness.run().status == "proposal"
    assert len(harness.tool_calls) == 1 and harness.tool_calls[0]["arguments"]["request"] == evidence_request


@pytest.mark.parametrize("usage", [{"input_tokens": True, "output_tokens": 5},
                                    {"input_tokens": -1, "output_tokens": 5}, None])
def test_invalid_usage_preserves_the_call_reservation(harness, usage):
    harness.transform_reply = lambda response: response.update(usage=usage)
    assert harness.run().reason == "invalid_model_response"
    assert harness.budget["reserved_model_turns"] == 1
    assert harness.budget["input_tokens"] == harness.budget["output_tokens"] == 0
    assert harness.events()[-1]["details"]["recovery"]["steps"][-1]["usage"] is None


@pytest.mark.parametrize("field,limit,reason", [
    ("max_output_bytes", 10, "model_output_limit"),
    ("max_observation_bytes", 5, "observation_byte_limit"),
    ("max_input_bytes", 3000, "model_input_limit"),
])
def test_byte_limits_stop_further_calls_without_losing_known_usage(build, field, limit, reason):
    harness = build(replace(POLICY, **{field: limit}))
    if field == "max_input_bytes":
        harness.observation = {"text": "x" * 5000}
    result = harness.run()
    assert (result.status, result.reason) == ("budget_exhausted", reason)
    assert len(harness.model_calls) == 1 and harness.budget["input_tokens"] == 10
    assert len(harness.tool_calls) == (field != "max_output_bytes")


@pytest.mark.parametrize("uncertain", [False, True])
def test_expected_adapter_failure_is_not_retried_and_reservation_survives_resume(harness, uncertain):
    async def fail(request, timeout):
        harness.model_calls.append(request)
        raise RecoveryActionError("service_unavailable", uncertain=uncertain)

    result = harness.run(decide=fail)
    assert result.status == ("uncertain" if uncertain else "unresolved")
    assert harness.budget["reserved_model_turns"] == 1
    assert harness.budget["completed_model_turns"] == 0
    assert harness.budget["consumed_cases"] == (0 if uncertain else 1)
    before = harness.budget
    harness.resume()
    assert harness.run(decide=fail) == result
    assert harness.budget == before and len(harness.model_calls) == 1


@pytest.mark.parametrize("code,uncertain", [("evidence_location_invalid", True), ("evidence_task_mismatch", False)])
def test_uncertain_or_wrong_task_evidence_errors_stop_without_model_feedback(harness, code, uncertain):
    async def fail(request, timeout):
        harness.tool_calls.append(request)
        raise RecoveryActionError(code, uncertain=uncertain)
    result = harness.run(execute=fail)
    assert result.status == ("uncertain" if uncertain else "unresolved")
    assert len(harness.model_calls) == len(harness.tool_calls) == 1


def test_timeout_cancels_the_adapter_and_keeps_its_reservation(build):
    harness = build(replace(POLICY, max_call_seconds=0.02))
    cancelled = []

    async def slow(request, timeout):
        harness.model_calls.append(request)
        try:
            await asyncio.sleep(10)
        finally:
            cancelled.append(True)

    result = harness.run(decide=slow)
    assert (result.status, result.reason) == ("uncertain", "action_timeout")
    assert cancelled == [True]
    assert harness.budget["reserved_model_turns"] == 1 and harness.budget["consumed_cases"] == 0
    assert harness.events()[-1]["details"]["recovery"]["elapsed_seconds"] >= 0.02
    assert harness.run(decide=slow) == result and len(harness.model_calls) == 1


@pytest.mark.parametrize("phase", ["model_intent", "before_observation_save", "model_observation",
                                  "tool_observation", "final_result"])
def test_resume_replays_saved_observations_and_preserves_uncertain_calls(harness, phase):
    _interrupt_on_save(harness.store, phase)
    with pytest.raises(_Interrupted):
        harness.run()
    harness.resume()
    result = harness.run()
    unknown = phase in {"model_intent", "before_observation_save"}
    assert result.status == ("uncertain" if unknown else "proposal")
    assert len(harness.model_calls) == (0 if phase == "model_intent" else 1 if unknown else 2)
    assert len(harness.tool_calls) == (0 if unknown else 1)
    assert harness.budget["reserved_cases"] == 1
    assert harness.budget["reserved_model_turns"] == (1 if unknown else 2)
    assert harness.budget["consumed_cases"] == (0 if unknown else 1)
    assert len(result.event_ids) == (2 if phase in {"model_observation", "tool_observation"} else 1)
    before = harness.budget
    assert harness.run() == result and harness.budget == before


def test_active_time_allowance_survives_restart_without_charging_downtime(build, monkeypatch):
    harness = build(replace(POLICY, max_case_seconds=1))
    clock = [0.0]
    monkeypatch.setattr(recovery, "monotonic", lambda: clock[0])
    timeouts = []

    async def decide(request, timeout):
        timeouts.append(timeout)
        clock[0] += 0.2
        return await harness.decide(request, timeout)

    async def execute(request, timeout):
        clock[0] += 0.2
        return await harness.execute(request, timeout)

    _interrupt_on_save(harness.store, "tool_observation")
    with pytest.raises(_Interrupted):
        harness.run(decide=decide, execute=execute)
    clock[0] += 1000
    harness.resume()
    assert harness.run(decide=decide, execute=execute).status == "proposal"
    assert timeouts == pytest.approx([1.0, 0.6])
    assert harness.events()[-1]["details"]["recovery"]["elapsed_seconds"] == pytest.approx(0.6)
    assert len(harness.tool_calls) == 1


@pytest.mark.parametrize("phase", ["model_intent", "model_observation"])
def test_storage_failure_stops_immediately_without_follow_up_writes(harness, monkeypatch, phase):
    writes = []
    original = harness.store.update_event

    def record(*args, **kwargs):
        writes.append(args[1])
        steps = kwargs.get("details", {}).get("recovery", {}).get("steps", [])
        if steps and steps[-1]["status"] == ("started" if phase == "model_intent" else "succeeded"):
            raise sqlite3.OperationalError("fixture disk failure")
        return original(*args, **kwargs)

    monkeypatch.setattr(harness.store, "update_event", record)
    with pytest.raises(sqlite3.OperationalError, match="fixture disk failure"):
        harness.run()
    assert writes == (["started", "started"] if phase == "model_intent" else ["started", "started", "started"])
    assert len(harness.model_calls) == (phase == "model_observation")
    assert harness.budget["reserved_model_turns"] == (phase == "model_observation")
    assert not harness.tool_calls
    assert harness.store.get_run("run")["status"] == "running"


def test_additional_fetch_evidence_preserves_the_primary_source_checkpoint(harness):
    source_id = harness.case.source_event_ids[0]
    additional = []

    async def fetch(request, timeout):
        event_id = harness.store.start_event(request["task_id"], "fetch")
        harness.store.update_event(event_id, "succeeded", body=b'{"sku":"0001"}',
                                    details={"http_status": 200, "body_complete": True})
        additional.append(event_id)
        return {"source_event_id": event_id, "value": {"sku": "0001"}}

    assert harness.run(execute=fetch).status == "proposal"
    assert harness.store.get_task(harness.case.task_id)["checkpoint"]["source_event_id"] == source_id
    assert {source["source_event_id"] for source in harness.model_calls[-1]["sources"]} == {source_id, *additional}


def test_policy_mismatch_and_cross_task_sources_fail_before_recovery_side_effects(harness):
    other = harness.add_case()
    before = (harness.budget, harness.events(), harness.store.get_task(harness.case.task_id))
    with pytest.raises(ValueError, match="original saved recovery policy"):
        asyncio.run(run_recovery(harness.store, harness.case,
                                 policy=replace(POLICY, service_version="changed"),
                                 decide=harness.decide, execute=harness.execute))
    with pytest.raises(ValueError, match="on this task"):
        harness.run(replace(harness.case, source_event_ids=other.source_event_ids))
    assert (harness.budget, harness.events(), harness.store.get_task(harness.case.task_id)) == before
    assert not harness.model_calls and not harness.tool_calls


@pytest.mark.parametrize("outcome", ["proposal", "unresolved"])
def test_recovery_preserves_previously_accepted_partial_records(harness, outcome):
    task_id = harness.case.task_id
    event_id = harness.store.start_event(task_id, "extract")
    item = {"product_name": "Fixture gloves", "product_url": harness.store.get_task(task_id)["target_key"],
            "captured_at": "2026-10-20T00:00:00Z", "sku": "retained", "item_name": "Previously accepted item"}
    harness.store.commit_product(event_id, [item], status="partial", issues=harness.case.issues)
    before = harness.store.list_items(task_id)
    harness.store.set_task_state(task_id, "pending", stage="recovery")
    if outcome == "unresolved":
        harness.decision = lambda request: {"action": "finish_unresolved", "arguments": {"reason": "No more records"}}
    assert harness.run().status == outcome
    assert harness.store.list_items(task_id) == before


def test_progress_logs_expose_actions_and_outcomes_without_raw_evidence(harness, caplog):
    caplog.set_level(logging.INFO, logger="safco_catalog_scraper.recovery")
    harness.observation = {"text": "PRIVATE_FIXTURE_WORDING"}
    assert harness.run().status == "proposal"
    for message in ("Recovery started", "Recovery action", "Recovery observation", "Recovery outcome"):
        assert message in caplog.text
    assert "remaining_model_turns=" in caplog.text and "task_id=" in caplog.text
    assert "PRIVATE_FIXTURE_WORDING" not in caplog.text
    assert "PRIVATE_FIXTURE_WORDING" in json.dumps(harness.events())


_CRASH_WORKER = """
import os
import runpy
import signal
import sys

fixtures = runpy.run_path(sys.argv[1])
harness = fixtures['_Harness'](sys.argv[2])
fixtures['_interrupt_on_save'](harness.store, sys.argv[3],
                               lambda: os.kill(os.getpid(), signal.SIGKILL))
original_decide, original_execute = harness.decide, harness.execute

async def decide(request, timeout):
    print('MODEL', flush=True)
    return await original_decide(request, timeout)

async def execute(request, timeout):
    print('TOOL', flush=True)
    return await original_execute(request, timeout)

harness.run(decide=decide, execute=execute)
raise AssertionError('Crash point was not reached')
"""


@pytest.mark.skipif(not hasattr(signal, "SIGKILL"), reason="Requires an uncatchable process exit")
@pytest.mark.parametrize("phase", ["model_intent", "tool_observation", "final_result"])
def test_sigkill_resumes_without_duplicate_calls_or_resetting_allowances(tmp_path, build, phase):
    baseline = build()
    expected = baseline.run()
    path = tmp_path / "killed.sqlite"
    environment = dict(os.environ, PYTHONDONTWRITEBYTECODE="1",
                       PYTHONPATH=str(Path(__file__).resolve().parents[1] / "src"))
    killed = subprocess.run([sys.executable, "-B", "-c", _CRASH_WORKER,
                             str(Path(__file__).resolve()), str(path), phase],
                            env=environment, capture_output=True, text=True, timeout=15)
    assert killed.returncode == -signal.SIGKILL, killed.stderr
    assert killed.stdout.count("MODEL") == (0 if phase == "model_intent" else 2 if phase == "final_result" else 1)
    assert killed.stdout.count("TOOL") == (0 if phase == "model_intent" else 1)
    with Storage(path) as store:
        store.resume_run("run", {"recovery": POLICY.as_dict()}, CODE_VERSION)
        case = RecoveryCase(1, ({"code": "missing_masterData"},), (1,))
        model_calls = []

        async def decide(request, timeout):
            model_calls.append(request)
            return _reply(request, _proposal(request["history"][-1]["evidence_id"]))

        async def execute(request, timeout):
            pytest.fail("Saved evidence must not be collected again")

        result = asyncio.run(run_recovery(store, case, policy=POLICY, decide=decide, execute=execute))
        assert result.status == ("uncertain" if phase == "model_intent" else "proposal")
        assert len(model_calls) == (phase == "tool_observation")
        budget = store.get_run("run")["budget"]
        assert budget["reserved_cases"] == 1
        assert budget["reserved_model_turns"] == (1 if phase == "model_intent" else 2)
        assert not store.list_items(case.task_id)
        if phase != "model_intent":
            assert result.candidates == expected.candidates
            assert budget == baseline.budget
