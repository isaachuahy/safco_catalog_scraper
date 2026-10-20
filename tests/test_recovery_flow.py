"""Offline endpoint-to-catalog recovery, rejection and forced-process-exit contracts."""

import importlib.util
import json
import os
import signal
import subprocess
import sys
from collections import Counter
from dataclasses import replace
from pathlib import Path

import httpx
import pytest

from safco_catalog_scraper.recovery import RecoveryPolicy
from safco_catalog_scraper.runner import run_catalog
from safco_catalog_scraper.storage import Storage
from test_runner import _Clock, _FixtureSite, _config, _set_clock, PRODUCT_URLS, GLOVES_URL

_spec = importlib.util.spec_from_file_location("decision_endpoint", Path(__file__).parents[1] / "api/recovery.py")
endpoint = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(endpoint)
POLICY = RecoveryPolicy(endpoint.SERVICE_VERSION, "fixture-model", endpoint.PROMPT_VERSION)
SETTINGS = {"SAFCO_RECOVERY_TOKEN": "fixture-reviewer-token", "OPENAI_API_KEY": "fixture-provider-secret",
            "SAFCO_RECOVERY_MODEL": POLICY.model}


def _proposal(source_id, kind="items", locator="script:0:variantData"):
    return {"action": "submit_candidates", "arguments": {"candidates": [
        {"record": {"kind": kind}, "evidence": [{"evidence_id": "source:" + str(source_id), "locator": locator}]}]}}


def _correct_lookup(request):
    source_id = request["case"]["sources"][0]["source_event_id"]
    history = request["history"]
    if not history:
        arguments = {"source_event_id": source_id, "view": "scripts"}
    elif len(history) == 2:
        arguments = {"source_event_id": source_id, "view": "json", "locator": "script:0:variantData#/"}
    elif len(history) == 4:
        assert history[-1]["observation"] == {"error": {"code": "evidence_location_invalid"}}
        arguments = {"source_event_id": source_id, "view": "json", "locator": "script:0:variantData"}
    else:
        return _proposal(source_id)
    return {"action": "read_source", "arguments": arguments}


def _collect(path, *, resume=False, counter=None, crash_phase=None, decision=None, mutate=None,
             policy=POLICY, public_demo=False):
    clock = _Clock()
    patch = pytest.MonkeyPatch()
    _set_clock(patch, clock)
    site = _FixtureSite(clock)
    site.products[PRODUCT_URLS[0]] = site.products[PRODUCT_URLS[0]].replace(b"window.masterData", b"window.variantData")
    if mutate:
        mutate(site)
    calls = []
    settings = dict(SETTINGS)
    access_token = settings["SAFCO_RECOVERY_TOKEN"]
    if public_demo:
        settings.pop("SAFCO_RECOVERY_TOKEN")
        settings.update(SAFCO_RECOVERY_ACCESS_MODE="public_demo",
                        SAFCO_RECOVERY_DEMO_EXPIRES_AT="2099-01-01T00:00:00Z")
        access_token = None

    def provider(request):
        payload = json.loads(request.content)
        assert payload["store"] is False and payload["max_output_tokens"] == 2048
        assert request.headers["Authorization"] == "Bearer fixture-provider-secret"
        case = json.loads(payload["input"])
        calls.append(case["request_id"])
        if counter:
            with Path(counter).open("a") as stream:
                stream.write(case["request_id"] + "\n")
        source_id = case["case"]["sources"][0]["source_event_id"]
        if decision:
            action = decision(case)
        elif not case["history"]:
            action = {"action": "read_source", "arguments": {"source_event_id": source_id, "view": "scripts"}}
        elif len(case["history"]) == 2:
            action = {"action": "read_source", "arguments": {"source_event_id": source_id,
                       "view": "json", "locator": "script:0:variantData"}}
        else:
            action = _proposal(source_id)
        arguments = action["arguments"] if action["action"] != "fetch_evidence" else {}
        return httpx.Response(200, json={"id": "fixture-provider-request", "status": "completed",
            "output": [{"type": "function_call", "name": action["action"], "arguments": json.dumps(arguments)}],
            "usage": {"input_tokens": 10, "output_tokens": 5}})

    def gateway(request):
        assert str(request.url) == "https://recovery.example/api/recovery"
        if public_demo:
            assert "authorization" not in request.headers
        status, response = endpoint.decide(json.loads(request.content), token=request.headers.get("Authorization", ""),
                                          settings=settings, transport=httpx.MockTransport(provider))
        return httpx.Response(status, json=response)

    original_update, original_commit = Storage.update_event, Storage.commit_product
    def update(store, event, status, **kwargs):
        state = (kwargs.get("details") or {}).get("recovery")
        step = state["steps"][-1] if state and state["steps"] else None
        if crash_phase == "unsaved_model_response" and step and step["kind"] == "model" and step["status"] == "succeeded":
            os.kill(os.getpid(), signal.SIGKILL)
        original_update(store, event, status, **kwargs)
        if (crash_phase == "saved_read_error" and step and step["kind"] == "evidence"
                and step["status"] == "failed" and step.get("error_code") == "evidence_location_invalid"):
            os.kill(os.getpid(), signal.SIGKILL)
        if ((crash_phase == "saved_observation" and step and step["kind"] == "evidence" and step["status"] == "succeeded")
                or (crash_phase == "saved_proposal" and state and state["outcome"] == "proposal")):
            os.kill(os.getpid(), signal.SIGKILL)
    def commit(store, event, items, **kwargs):
        recovered = bool(items and items[0]["sku"] == "000000")
        if recovered and crash_phase == "before_acceptance":
            os.kill(os.getpid(), signal.SIGKILL)
        result = original_commit(store, event, items, **kwargs)
        if recovered and crash_phase == "after_acceptance":
            os.kill(os.getpid(), signal.SIGKILL)
        return result
    patch.setattr(Storage, "update_event", update)
    patch.setattr(Storage, "commit_product", commit)
    try:
        config = _config(recovery_endpoint="https://recovery.example/api/recovery", recovery_policy=policy)
        report = run_catalog(path, "run", config, resume=resume, transport=httpx.MockTransport(site),
                             recovery_access_token=access_token,
                             recovery_transport=httpx.MockTransport(gateway))
        with Storage(path) as store:
            rows = [{key: value for key, value in row.items() if key not in {"item_id", "extraction_event_id"}}
                    for task in store.list_tasks("run") for row in store.list_items(task["task_id"])]
        return report, rows, calls, site
    finally:
        patch.undo()


@pytest.mark.parametrize("public_demo", [False, True])
def test_saved_evidence_reaches_validated_catalog_through_the_http_endpoint(tmp_path, public_demo):
    path = tmp_path / "catalog.sqlite"
    report, rows, calls, site = _collect(path, public_demo=public_demo)
    assert report["status"] == "complete" and report["accepted_items"] == 4
    assert report["recovery"]["outcomes"] == {"proposal": 1}
    assert report["recovery"]["budget"]["reserved_cases"] == 1
    assert report["recovery"]["budget"]["completed_model_turns"] == 3
    assert len(calls) == len(set(calls)) == 3
    recovered = next(row for row in rows if row["sku"] == "000000")
    assert recovered["base_price"] == "16.49"
    assert recovered["quantity_tiers"] == [{"min_quantity": 10, "unit_price": "15.99"}]
    assert site.product_calls == Counter(dict.fromkeys(PRODUCT_URLS, 1))
    assert all(row["count_status"] == "matched" for row in report["item_coverage"])
    with Storage(path) as store:
        assert SETTINGS["OPENAI_API_KEY"] not in json.dumps(store.get_run("run"))
        assert SETTINGS["SAFCO_RECOVERY_TOKEN"] not in json.dumps(store.get_run("run"))


def test_captured_bad_root_locator_can_be_corrected_within_four_model_turns(tmp_path):
    path = tmp_path / "corrected.sqlite"
    report, rows, calls, site = _collect(path, decision=_correct_lookup)
    assert report["status"] == "complete" and len(rows) == 4
    assert len(calls) == 4 and report["recovery"]["budget"]["completed_model_turns"] == 4
    assert site.product_calls[PRODUCT_URLS[0]] == 1
    with Storage(path) as store:
        event = next(event for task in store.list_tasks("run") for event in store.list_events(task["task_id"])
                     if event["type"] == "recovery")
    failed = [step for step in event["details"]["recovery"]["steps"] if step["status"] == "failed"]
    assert len(failed) == 1 and failed[0]["error_code"] == "evidence_location_invalid"
    assert failed[0]["input"]["arguments"]["locator"] == "script:0:variantData#/"
    assert report["recovery"]["cases"][0]["tool_errors"] == [{
        "step_id": failed[0]["step_id"], "action": "read_source", "code": "evidence_location_invalid"}]
    assert [step["input"]["remaining_model_turns"] for step in event["details"]["recovery"]["steps"]
            if step["kind"] == "model"] == [4, 3, 2, 1]
    assert [step["evidence_id"] for step in event["details"]["recovery"]["steps"]
            if step["kind"] == "evidence"] == ["observation:2", "observation:4", "observation:6"]


def test_repeated_bad_locators_stop_at_the_original_model_budget(tmp_path):
    def bad(request):
        return {"action": "read_source", "arguments": {
            "source_event_id": request["case"]["sources"][0]["source_event_id"],
            "view": "json", "locator": "script:0:variantData#/"}}
    report, _, calls, _ = _collect(tmp_path / "budget.sqlite", decision=bad)
    assert len(calls) == 4
    assert report["recovery"]["cases"][0]["reason"] == "case_model_turn_limit"


def test_forced_exit_after_saved_lookup_error_resumes_without_repeating_calls(tmp_path):
    baseline, expected_rows, baseline_calls, _ = _collect(tmp_path / "baseline.sqlite", decision=_correct_lookup)
    path, counter = tmp_path / "resumed.sqlite", tmp_path / "calls.txt"
    script = ("from test_recovery_flow import _collect, _correct_lookup; import sys; "
              "_collect(sys.argv[1], counter=sys.argv[2], crash_phase='saved_read_error', decision=_correct_lookup)")
    environment = dict(os.environ, PYTHONPATH=os.pathsep.join([str(Path(__file__).parents[1] / "src"), str(Path(__file__).parent)]),
                       PYTHONDONTWRITEBYTECODE="1")
    worker = subprocess.run([sys.executable, "-B", "-c", script, str(path), str(counter)],
                            env=environment, capture_output=True, timeout=20)
    assert worker.returncode == -signal.SIGKILL, worker.stderr.decode()
    report, rows, _, _ = _collect(path, resume=True, counter=counter, decision=_correct_lookup)
    assert rows == expected_rows and report == baseline
    assert counter.read_text().splitlines() == baseline_calls


@pytest.mark.parametrize("phase", ["saved_observation", "saved_proposal", "before_acceptance", "after_acceptance"])
def test_forced_exit_resumes_to_equivalent_catalog_and_report(tmp_path, phase):
    baseline, expected_rows, baseline_calls, _ = _collect(tmp_path / "baseline.sqlite")
    path, counter = tmp_path / "resumed.sqlite", tmp_path / "calls.txt"
    script = "from test_recovery_flow import _collect; import sys; _collect(sys.argv[1], counter=sys.argv[2], crash_phase=sys.argv[3])"
    environment = dict(os.environ, PYTHONPATH=os.pathsep.join([str(Path(__file__).parents[1] / "src"), str(Path(__file__).parent)]),
                       PYTHONDONTWRITEBYTECODE="1")
    worker = subprocess.run([sys.executable, "-B", "-c", script, str(path), str(counter), phase],
                            env=environment, capture_output=True, timeout=20)
    assert worker.returncode == -signal.SIGKILL, worker.stderr.decode()
    report, rows, _, site = _collect(path, resume=True, counter=counter)
    assert rows == expected_rows
    assert report == baseline
    assert counter.read_text().splitlines() == baseline_calls
    assert site.product_calls[PRODUCT_URLS[0]] == 0


def test_unsaved_remote_result_is_not_resubmitted_after_restart(tmp_path):
    path, counter = tmp_path / "uncertain.sqlite", tmp_path / "calls.txt"
    script = "from test_recovery_flow import _collect; import sys; _collect(sys.argv[1], counter=sys.argv[2], crash_phase='unsaved_model_response')"
    environment = dict(os.environ, PYTHONPATH=os.pathsep.join([str(Path(__file__).parents[1] / "src"), str(Path(__file__).parent)]))
    worker = subprocess.run([sys.executable, "-B", "-c", script, str(path), str(counter)],
                            env=environment, capture_output=True, timeout=20)
    assert worker.returncode == -signal.SIGKILL, worker.stderr.decode()
    report, rows, calls, _ = _collect(path, resume=True, counter=counter)
    assert report["status"] == "partial" and len(rows) == 3
    assert report["recovery"]["outcomes"] == {"uncertain": 1}
    assert len(counter.read_text().splitlines()) == 1 and calls == []


def test_model_supplied_catalog_values_are_rejected(tmp_path):
    def invent(request):
        value = _proposal(request["case"]["sources"][0]["source_event_id"])
        value["arguments"]["candidates"][0]["record"] = {"sku": "000000", "base_price": "0.01"}
        return value
    report, rows, _, _ = _collect(tmp_path / "reject.sqlite", decision=invent)
    assert len(rows) == 3 and report["status"] == "partial"
    assert any(issue["code"] == "recovery_candidate_rejected" for issue in report["unresolved"][0]["issues"])


def test_rejected_decision_keeps_bounded_data_and_safe_validation_locations(tmp_path):
    path = tmp_path / "invalid-decision.sqlite"
    decision = {"action": "submit_candidates", "arguments": {"candidates": [
        {"record": {"kind": "items"}, "evidence": [
            {"evidence_id": "source:999999", "locator": "script:0:variantData"}]}]}}
    report, _, _, _ = _collect(path, decision=lambda _: decision)
    assert report["recovery"]["cases"][0]["reason"] == "invalid_model_response"
    with Storage(path) as store:
        event = next(event for task in store.list_tasks("run") for event in store.list_events(task["task_id"])
                     if event["type"] == "recovery")
    step = event["details"]["recovery"]["steps"][-1]
    assert step["rejected_decision"] == decision
    assert step["diagnostic"]["error_type"] == "ValueError"
    assert step["diagnostic"]["locations"][-1]["function"] == "_decision"
    assert report["recovery"]["cases"][0]["diagnostic"] == step["diagnostic"]
    assert "unknown evidence" not in json.dumps(step["diagnostic"])
    assert not any("/" in location["file"] for location in step["diagnostic"]["locations"])


def test_budget_exhaustion_keeps_progress_and_reports_unattempted_recovery(tmp_path):
    report, rows, calls, _ = _collect(tmp_path / "limited.sqlite", policy=replace(POLICY, max_cases=0))
    assert len(rows) == 3 and calls == []
    assert report["recovery"]["cases"][0] == {
        "task_id": 5, "outcome": "budget_exhausted", "reason": "case_limit", "attempted": False,
        "task_outcome": "failed"}


@pytest.mark.parametrize("unknown", [True, False])
def test_reports_distinguish_unknown_total_from_known_shortfall(tmp_path, unknown):
    def mutate(site):
        html = site.products[PRODUCT_URLS[0]].replace(b"window.variantData", b"window.masterData")
        site.products[PRODUCT_URLS[0]] = (html.replace(b"window.productConfig", b"window.unknownConfig") if unknown
                                        else html.replace(b'\\"total\\": 1', b'\\"total\\": 2'))
    finish = lambda _: {"action": "finish_unresolved", "arguments": {"reason": "No additional source evidence"}}
    report, _, _, _ = _collect(tmp_path / "counts.sqlite", decision=finish, mutate=mutate)
    row = next(item for item in report["item_coverage"] if item["product_url"] == PRODUCT_URLS[0])
    assert row["decoded_item_count"] == 1
    assert row["count_status"] == ("unknown" if unknown else "shortfall")
    assert row["missing_source_rows"] == (None if unknown else 1)


@pytest.mark.parametrize("kind", ["category_config", "category_page"])
def test_discovery_recovery_uses_the_same_scope_and_pagination_validation(tmp_path, kind):
    def mutate(site):
        site.products[PRODUCT_URLS[0]] = site.products[PRODUCT_URLS[0]].replace(b"window.variantData", b"window.masterData")
        if kind == "category_config":
            site.category_html[GLOVES_URL] = site.category_html[GLOVES_URL].replace(b"window.algoliaConfig", b"window.categorySettings")
        else:
            site.pages[(GLOVES_URL, 0)] = {"payload": site.pages[(GLOVES_URL, 0)]}
    def decide(request):
        return _proposal(request["case"]["sources"][0]["source_event_id"], kind,
                         "script:0:categorySettings" if kind == "category_config" else "body#/payload")
    report, rows, calls, _ = _collect(tmp_path / "discovery.sqlite", mutate=mutate, decision=decide)
    assert report["status"] == "complete" and len(rows) == 4 and len(calls) == 1
    assert all(category["complete"] for category in report["discovery"])


def test_aggregate_discovery_shortfall_is_persisted_and_sent_to_recovery(tmp_path):
    def mutate(site):
        site.products[PRODUCT_URLS[0]] = site.products[PRODUCT_URLS[0]].replace(b"window.variantData", b"window.masterData")
        site.pages[(GLOVES_URL, 1)]["results"][0]["hits"] = [{"objectID": "0", "url": PRODUCT_URLS[0]}]
    finish = lambda _: {"action": "finish_unresolved", "arguments": {"reason": "The saved category is inconsistent"}}
    report, _, calls, _ = _collect(tmp_path / "aggregate.sqlite", mutate=mutate, decision=finish)
    assert report["status"] == "partial" and len(calls) == 1
    assert report["discovery"][0]["count_status"] == "shortfall"
    assert report["discovery"][0]["missing_source_hits"] == 1
    assert report["unresolved"][0]["kind"] == "discovery"
    assert any(issue["code"] == "category_count_mismatch" for issue in report["unresolved"][0]["issues"])


def test_confirmed_empty_dataset_keeps_zero_counts_after_unresolved_recovery(tmp_path):
    def mutate(site):
        site.products[PRODUCT_URLS[0]] = (b'<h1>Empty family</h1><script>window.masterData = [];</script>'
            b'<script>window.productConfig = JSON.parse("{\\"total\\":0}");</script>')
    finish = lambda _: {"action": "finish_unresolved", "arguments": {"reason": "The dataset is empty"}}
    report, _, _, _ = _collect(tmp_path / "empty.sqlite", mutate=mutate, decision=finish)
    coverage = report["item_coverage"][0]
    assert coverage["expected_item_count"] == coverage["decoded_item_count"] == coverage["accepted_item_count"] == 0
    assert coverage["count_status"] == "matched" and coverage["missing_source_rows"] == 0
    assert report["unresolved"][0]["status"] == "failed"
