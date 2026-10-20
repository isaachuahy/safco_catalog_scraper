"""Offline command-line contracts, including real collection and saved-run resumption."""

import json
import logging
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import httpx
import pytest

import safco_catalog_scraper.__main__ as cli
import safco_catalog_scraper.runner as runner
from safco_catalog_scraper.runner import DEFAULT_CATEGORIES, RunConfig
from safco_catalog_scraper.storage import Storage
from test_runner import GLOVES_URL, PRODUCT_URLS, SURGICAL_URL, _Clock, _FixtureSite, _product_html, _set_clock


@pytest.mark.parametrize("access_token", ["reviewer-token", None])
def test_cli_recovers_and_resumes_with_saved_policy_despite_changed_environment(
        tmp_path, site, monkeypatch, capsys, access_token):
    path, output = tmp_path / "hosted.sqlite", tmp_path / "catalog.json"
    site.products[PRODUCT_URLS[0]] = site.products[PRODUCT_URLS[0]].replace(b"window.masterData", b"window.variantData")
    if access_token is None:
        monkeypatch.delenv("SAFCO_RECOVERY_TOKEN", raising=False)
    else:
        monkeypatch.setenv("SAFCO_RECOVERY_TOKEN", access_token)
    calls = []
    def decide(request):
        assert request.headers.get("Authorization") == ("Bearer " + access_token if access_token else None)
        payload = json.loads(request.content)
        calls.append((str(request.url), payload["expected"]))
        source = payload["case"]["sources"][0]["source_event_id"]
        decision = ({"action": "read_source", "arguments": {"source_event_id": source, "view": "scripts"}}
                    if not payload["history"] else {"action": "submit_candidates", "arguments": {"candidates": [
                        {"record": {"kind": "items"}, "evidence": [{"evidence_id": "source:" + str(source),
                         "locator": "script:0:variantData"}]}]}})
        return httpx.Response(200, json={"request_id": payload["request_id"], **payload["expected"],
                                         "decision": decision, "usage": {"input_tokens": 10, "output_tokens": 5}})
    collect = cli.run_catalog
    monkeypatch.setattr(cli, "run_catalog", lambda *args, **kwargs: collect(
        *args, **kwargs, recovery_transport=httpx.MockTransport(decide)))
    original = Storage.update_event
    interrupted = []
    def save(store, event, status, **kwargs):
        original(store, event, status, **kwargs)
        state = (kwargs.get("details") or {}).get("recovery")
        if state and state["steps"] and state["steps"][-1]["kind"] == "evidence" and not interrupted:
            if state["steps"][-1]["status"] == "succeeded":
                interrupted.append(True)
                raise KeyboardInterrupt()
    monkeypatch.setattr(Storage, "update_event", save)
    assert cli.main(_arguments(path) + ["--recovery-endpoint", "https://original.example/api/recovery",
        "--recovery-model", "original-model", "--output", str(output)]) == 130
    assert capsys.readouterr().out == ""
    monkeypatch.setenv("SAFCO_RECOVERY_ENDPOINT", "https://changed.example/api/recovery")
    monkeypatch.setenv("SAFCO_RECOVERY_MODEL", "changed-model")
    assert cli.main(_resume_arguments(path) + ["--output", str(output)]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["accepted_items"] == 4 and report["recovery"]["outcomes"] == {"proposal": 1}
    assert len(calls) == 2 and all(url == "https://original.example/api/recovery" for url, _ in calls)
    assert all(expected["model"] == "original-model" for _, expected in calls)
    assert json.loads(output.read_text())["run"]["status"] == "complete"
    assert site.product_calls[PRODUCT_URLS[0]] == 1


def test_cli_recover_enables_a_bounded_policy_without_refetching_saved_products(
        tmp_path, site, monkeypatch, capsys):
    path = tmp_path / "recover.sqlite"
    site.products[PRODUCT_URLS[0]] = _product_html("000000", name="Unfamiliar item", description="Assorted")
    assert cli.main(_arguments(path, limit=1)) == 1
    capsys.readouterr()
    before_requests = len(site.requests)
    monkeypatch.setenv("SAFCO_RECOVERY_TOKEN", "reviewer-token")
    assert cli.main(["recover", "--database", str(path), "--run-id", "run",
                     "--recovery-endpoint", "https://recovery.example/api/recovery",
                     "--recovery-model", "fixture-model", "--recovery-case-limit", "0"]) == 1
    report = json.loads(capsys.readouterr().out)
    assert report["accepted_items"] == 1 and report["recovery"]["mode"] == "hosted"
    assert report["recovery"]["cases"][0]["attempted"] is False
    assert len(site.requests) == before_requests
    with Storage(path) as store:
        saved = store.get_run("run")
        assert saved["config"]["unresolved_product_limit"] == 1
        assert saved["config"]["recovery"]["max_cases"] == 0
        assert saved["config"]["recovery"]["model"] == "fixture-model"


def test_cli_recover_requires_existing_work_and_endpoint_configuration(tmp_path, site, monkeypatch, capsys):
    path = tmp_path / "missing.sqlite"
    assert cli.main(["recover", "--database", str(path), "--run-id", "run"]) == 2
    assert not path.exists()
    capsys.readouterr()
    site.products[PRODUCT_URLS[0]] = _product_html("000000", name="Unfamiliar item", description="Assorted")
    assert cli.main(_arguments(path, limit=1)) == 1
    capsys.readouterr()
    with Storage(path) as store:
        before = store.get_run("run")
    monkeypatch.delenv("SAFCO_RECOVERY_ENDPOINT", raising=False)
    monkeypatch.delenv("SAFCO_RECOVERY_MODEL", raising=False)
    assert cli.main(["recover", "--database", str(path), "--run-id", "run"]) == 2
    assert capsys.readouterr().out == ""
    with Storage(path) as store:
        assert store.get_run("run") == before


def _arguments(path, *, limit=10):
    return ["run", "--database", str(path), "--run-id", "run", "--unresolved-product-limit", str(limit),
            "--request-interval-seconds", "0", "--retry-base-seconds", "0"]


def _resume_arguments(path, run_id="run"):
    return ["resume", "--database", str(path), "--run-id", run_id]


def _export_arguments(path, output, run_id="run"):
    return ["export", "--database", str(path), "--run-id", run_id, "--output", str(output)]


@pytest.fixture(autouse=True)
def restore_transport_logging():
    levels = {name: logging.getLogger(name).level for name in ("httpx", "httpcore")}
    yield
    for name, level in levels.items():
        logging.getLogger(name).setLevel(level)


@pytest.fixture
def site(monkeypatch):
    clock = _Clock()
    _set_clock(monkeypatch, clock)
    value = _FixtureSite(clock)
    collect = cli.run_catalog

    def offline(*args, **kwargs):
        return collect(*args, **kwargs, transport=httpx.MockTransport(value))

    monkeypatch.setattr(cli, "run_catalog", offline)
    return value


def _interrupt_once(site):
    def reply(attempt):
        if attempt == 1:
            raise KeyboardInterrupt()
        return httpx.Response(200, content=site.products[PRODUCT_URLS[0]])

    site.replies[PRODUCT_URLS[0]] = reply


def test_default_settings_and_report_pass_through_the_cli(tmp_path, monkeypatch, capsys):
    path = tmp_path / "run.sqlite"
    report = {"run_id": "run", "status": "complete", "unresolved": []}
    calls = []

    def collect(database_path, run_id, config, *, resume, recovery_access_token):
        calls.append((database_path, run_id, config, resume))
        return report

    monkeypatch.setattr(cli, "run_catalog", collect)
    result = cli.main(["run", "--database", str(path), "--run-id", "run"])
    assert result == 0
    assert calls == [(path, "run", RunConfig(), False)]
    assert calls[0][2].categories == DEFAULT_CATEGORIES
    assert json.loads(capsys.readouterr().out) == report


def test_collection_options_are_saved_as_the_runs_policy(tmp_path, site, capsys):
    path = tmp_path / "run.sqlite"
    options = ["--category", SURGICAL_URL, "--category", GLOVES_URL,
               "--max-fetch-attempts", "2", "--retry-max-seconds", "3",
               "--timeout-seconds", "4", "--max-response-bytes", "8000"]
    assert cli.main(_arguments(path, limit=7) + options) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "complete"
    with Storage(path) as store:
        settings = store.get_run("run")["config"]
    assert settings == RunConfig(
        unresolved_product_limit=7, categories=(SURGICAL_URL, GLOVES_URL),
        max_fetch_attempts=2, request_interval_seconds=0, retry_base_seconds=0,
        retry_max_seconds=3, timeout_seconds=4, max_response_bytes=8000,
    ).as_dict()


def test_category_override_limits_discovery_and_collection(tmp_path, site, capsys):
    assert cli.main(_arguments(tmp_path / "run.sqlite") + ["--category", GLOVES_URL]) == 0
    report = json.loads(capsys.readouterr().out)
    assert [category["category_url"] for category in report["discovery"]] == [GLOVES_URL]
    assert report["accepted_items"] == report["discovered_products"] == 3
    assert report["fetch_attempts"] == 6
    assert all(str(request.url) != SURGICAL_URL for request in site.requests)
    assert PRODUCT_URLS[3] not in site.product_calls


@pytest.mark.parametrize("mode,limit,status,accepted,remaining", [
    ("partial", 10, "partial", 4, 0),
    ("partial", 1, "partial", 1, 3),
    ("failed", 1, "failed", 0, 3),
])
def test_unresolved_collection_returns_a_nonzero_code_and_a_report(
        tmp_path, site, capsys, mode, limit, status, accepted, remaining):
    path = tmp_path / "run.sqlite"
    site.products[PRODUCT_URLS[0]] = (
        _product_html("000000", name="Unfamiliar item", description="Assorted")
        if mode == "partial" else b"<h1>Missing dataset</h1>")
    output = tmp_path / "catalog.json"
    assert cli.main(_arguments(path, limit=limit) + ["--output", str(output)]) == 1
    report = json.loads(capsys.readouterr().out)
    assert report["status"] == status
    assert report["accepted_items"] == accepted
    assert len(report["remaining_product_urls"]) == remaining
    assert len(report["unresolved"]) == 1
    assert report["recovery"]["mode"] == "deferred"
    assert report["catalog_export"] == {"status": "succeeded", "output_path": str(output.resolve())}
    catalog = json.loads(output.read_bytes())
    assert catalog["run"]["status"] == status
    assert sum(len(product["items"]) for product in catalog["products"]) == accepted
    with Storage(path) as store:
        assert store.get_run("run")["status"] == status


@pytest.mark.parametrize("arguments", [[], ["run"], ["resume"], ["unknown"]])
def test_missing_arguments_and_unknown_commands_exit_before_collection(monkeypatch, capsys, arguments):
    monkeypatch.setattr(cli, "run_catalog", lambda *args, **kwargs: pytest.fail("Collection must not start"))
    with pytest.raises(SystemExit) as caught:
        cli.main(arguments)
    assert caught.value.code == 2
    output = capsys.readouterr()
    assert output.out == ""
    assert "usage:" in output.err


@pytest.mark.parametrize("options", [
    ["--unresolved-product-limit", "many"], ["--max-fetch-attempts", "two"],
    ["--log-level", "TRACE"], ["--unexpected-option", "value"],
])
def test_malformed_options_exit_before_collection(tmp_path, monkeypatch, capsys, options):
    path = tmp_path / "run.sqlite"
    monkeypatch.setattr(cli, "run_catalog", lambda *args, **kwargs: pytest.fail("Collection must not start"))
    with pytest.raises(SystemExit) as caught:
        cli.main(_arguments(path) + options)
    assert caught.value.code == 2
    assert capsys.readouterr().out == ""
    assert not path.exists()


@pytest.mark.parametrize("options", [
    ["--unresolved-product-limit", "0"], ["--max-fetch-attempts", "-1"],
    ["--request-interval-seconds", "nan"], ["--retry-base-seconds", "inf"],
    ["--timeout-seconds", "0"], ["--max-response-bytes", "0"],
    ["--retry-base-seconds", "3", "--retry-max-seconds", "2"],
    ["--category", "https://example.com/catalog/gloves"],
    ["--category", GLOVES_URL, "--category", GLOVES_URL],
])
def test_invalid_policy_returns_two_without_creating_a_database(tmp_path, monkeypatch, capsys, caplog, options):
    path = tmp_path / "run.sqlite"
    monkeypatch.setattr(cli, "run_catalog", lambda *args, **kwargs: pytest.fail("Collection must not start"))
    assert cli.main(_arguments(path) + options) == 2
    assert capsys.readouterr().out == ""
    assert not path.exists()
    assert "Cannot run run_id=run" in caplog.text


def test_missing_resume_database_is_not_created(tmp_path, site, capsys, caplog):
    path = tmp_path / "missing.sqlite"
    assert cli.main(_resume_arguments(path)) == 2
    assert capsys.readouterr().out == ""
    assert not path.exists()
    assert not path.with_name(path.name + ".lock").exists()
    assert site.requests == []
    assert "Saved-run commands require an existing database" in caplog.text


def test_resume_restores_custom_policy_and_preserves_evidence_attempts_and_budget(tmp_path, site, capsys, caplog):
    path = tmp_path / "run.sqlite"
    _interrupt_once(site)
    options = ["--category", GLOVES_URL, "--max-fetch-attempts", "2", "--retry-max-seconds", "3",
               "--timeout-seconds", "4", "--max-response-bytes", "8000"]
    output = tmp_path / "catalog.json"
    assert cli.main(_arguments(path, limit=7) + options + ["--output", str(output)]) == 130
    assert capsys.readouterr().out == ""
    assert not output.exists()
    assert "use resume with the same database and run ID" in caplog.text
    budget = {"reserved_cases": 2, "consumed_cases": 1}
    with Storage(path) as store:
        saved = store.get_run("run")
        assert saved["status"] == "interrupted"
        product = next(task for task in store.list_tasks("run") if task["target_key"] == PRODUCT_URLS[0])
        event, = store.list_events(product["task_id"])
        store.update_event(event["event_id"], "started", budget=budget)
    discovery_calls = list(site.api_calls)
    assert cli.main(_resume_arguments(path) + ["--output", str(output)]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["status"] == "complete" and report["accepted_items"] == 3
    assert report["uncertain_fetches"] == 1 and report["fetch_attempts"] == 7
    assert report["recovery"]["budget"] == budget
    assert report["catalog_export"] == {"status": "succeeded", "output_path": str(output.resolve())}
    catalog = json.loads(output.read_bytes())
    assert catalog["run"]["status"] == "complete"
    assert catalog["run"]["budget"] == budget
    assert site.api_calls == discovery_calls
    assert site.product_calls[PRODUCT_URLS[0]] == 2
    with Storage(path) as store:
        resumed = store.get_run("run")
        assert resumed["status"] == "complete"
        assert resumed["config"] == saved["config"]
        assert resumed["config_hash"] == saved["config_hash"]
        assert resumed["budget"] == budget


@pytest.mark.parametrize("change", ["unknown_run", "changed_code", "policy_override"])
def test_incompatible_resume_cannot_change_saved_work(tmp_path, site, monkeypatch, capsys, change):
    path = tmp_path / "run.sqlite"
    _interrupt_once(site)
    assert cli.main(_arguments(path)) == 130
    capsys.readouterr()
    with Storage(path) as store:
        saved = store.get_run("run")
        tasks = store.list_tasks("run")
        events = [store.list_events(task["task_id"]) for task in tasks]
    calls = len(site.requests)
    arguments = _resume_arguments(path, "unknown" if change == "unknown_run" else "run")
    if change == "changed_code":
        monkeypatch.setattr(runner, "_code_version", lambda: "changed-code")
    if change == "policy_override":
        with pytest.raises(SystemExit) as caught:
            cli.main(arguments + ["--max-fetch-attempts", "20"])
        assert caught.value.code == 2
    else:
        assert cli.main(arguments) == 2
    assert capsys.readouterr().out == ""
    assert len(site.requests) == calls
    with Storage(path) as store:
        assert store.get_run("run") == saved
        assert store.list_tasks("run") == tasks
        assert [store.list_events(task["task_id"]) for task in tasks] == events


@pytest.mark.parametrize("status", ["complete", "partial", "failed"])
def test_finished_runs_cannot_be_restarted_or_resumed_implicitly(tmp_path, site, capsys, status):
    path = tmp_path / "run.sqlite"
    if status == "partial":
        site.products[PRODUCT_URLS[0]] = _product_html("000000", name="Unfamiliar item", description="Assorted")
    elif status == "failed":
        site.products[PRODUCT_URLS[0]] = b"<h1>Missing dataset</h1>"
    assert cli.main(_arguments(path, limit=1)) == (0 if status == "complete" else 1)
    capsys.readouterr()
    with Storage(path) as store:
        saved = store.get_run("run")
        rows = [store.list_items(task["task_id"]) for task in store.list_tasks("run")]
    calls = len(site.requests)
    for arguments in (_arguments(path), _resume_arguments(path)):
        assert cli.main(arguments) == 2
        assert capsys.readouterr().out == ""
    assert len(site.requests) == calls
    with Storage(path) as store:
        assert store.get_run("run") == saved
        assert [store.list_items(task["task_id"]) for task in store.list_tasks("run")] == rows


def test_another_collector_owning_the_database_returns_one_without_requests(tmp_path, site, capsys, caplog):
    path = tmp_path / "run.sqlite"
    with Storage(path):
        assert cli.main(_arguments(path)) == 1
    assert capsys.readouterr().out == ""
    assert site.requests == []
    assert "Another collector owns" in caplog.text


@pytest.mark.parametrize("failure", ["invalid_database", "invalid_parent"])
def test_storage_open_errors_return_one_without_a_report(tmp_path, site, capsys, failure):
    path = tmp_path / "run.sqlite"
    if failure == "invalid_database":
        path.write_bytes(b"Not a SQLite database")
    else:
        parent = tmp_path / "regular-file"
        parent.write_bytes(b"Not a directory")
        path = parent / "run.sqlite"
    assert cli.main(_arguments(path)) == 1
    assert capsys.readouterr().out == ""
    assert site.requests == []


def test_storage_commit_failure_returns_one_and_leaves_saved_source_resumable(tmp_path, site, monkeypatch, capsys):
    path = tmp_path / "run.sqlite"
    output = tmp_path / "catalog.json"

    def fail_commit(*args, **kwargs):
        raise sqlite3.OperationalError("fixture disk failure")

    with monkeypatch.context() as patch:
        patch.setattr(Storage, "commit_product", fail_commit)
        assert cli.main(_arguments(path) + ["--output", str(output)]) == 1
    assert capsys.readouterr().out == ""
    assert not output.exists()
    with Storage(path) as store:
        assert store.get_run("run")["status"] == "running"
    assert cli.main(_resume_arguments(path)) == 0
    assert json.loads(capsys.readouterr().out)["accepted_items"] == 4
    assert site.product_calls[PRODUCT_URLS[0]] == 1


@pytest.mark.parametrize("error_type", [AssertionError, ValueError, TypeError, KeyError])
def test_unexpected_error_returns_one_without_exposing_raw_diagnostics(
        tmp_path, site, capsys, caplog, error_type):
    path = tmp_path / "run.sqlite"

    def fail(attempt):
        raise error_type("Private fixture diagnostic details")

    site.replies[PRODUCT_URLS[0]] = fail
    assert cli.main(_arguments(path)) == 1
    output = capsys.readouterr().out
    diagnostic = json.loads(output)["diagnostic"]
    assert diagnostic["error_type"] == error_type.__name__
    assert diagnostic["locations"] and len(diagnostic["locations"]) <= 5
    assert all(set(frame) == {"file", "function", "line"} and "/" not in frame["file"]
               for frame in diagnostic["locations"])
    assert "Private fixture diagnostic details" not in output
    assert "error_type=" + error_type.__name__ in caplog.text
    assert "Private fixture diagnostic details" not in caplog.text
    with Storage(path) as store:
        assert store.get_run("run")["status"] == "interrupted"


@pytest.mark.parametrize("status,accepted,exit_code", [
    ("complete", 4, 0), ("partial", 1, 1), ("failed", 0, 1), ("interrupted", 0, 130),
])
def test_standalone_export_preserves_saved_run_status_without_collection_or_resumption(
        tmp_path, site, monkeypatch, capsys, status, accepted, exit_code):
    path = tmp_path / "run.sqlite"
    if status == "partial":
        site.products[PRODUCT_URLS[0]] = _product_html("000000", name="Unfamiliar item", description="Assorted")
    elif status == "failed":
        site.products[PRODUCT_URLS[0]] = b"<h1>Missing dataset</h1>"
    elif status == "interrupted":
        _interrupt_once(site)
    assert cli.main(_arguments(path, limit=1)) == exit_code
    capsys.readouterr()
    before = path.read_bytes()
    calls = len(site.requests)
    monkeypatch.setattr(cli, "run_catalog", lambda *args, **kwargs: pytest.fail("Export must not collect"))
    monkeypatch.setattr(runner, "_code_version", lambda: pytest.fail("Export must not check collector code"))
    monkeypatch.setattr(Storage, "resume_run", lambda *args, **kwargs: pytest.fail("Export must not resume"))
    output = tmp_path / "exports" / "2026-10-20.json"
    assert cli.main(_export_arguments(path, output)) == 0
    assert json.loads(capsys.readouterr().out) == {
        "run_id": "run", "catalog_export": {"status": "succeeded", "output_path": str(output.resolve())},
    }
    catalog = json.loads(output.read_bytes())
    assert catalog["run"]["status"] == status
    assert sum(len(product["items"]) for product in catalog["products"]) == accepted
    assert len(site.requests) == calls
    assert path.read_bytes() == before


@pytest.mark.parametrize("options", [[], ["--max-fetch-attempts", "2"],
                                    ["--category", GLOVES_URL], ["--unresolved-product-limit", "5"]])
def test_export_requires_an_output_and_rejects_collection_options(tmp_path, monkeypatch, capsys, options):
    path, output = tmp_path / "missing.sqlite", tmp_path / "catalog.json"
    arguments = _export_arguments(path, output)
    if not options:
        arguments = arguments[:-2]
    monkeypatch.setattr(cli, "export_catalog", lambda *args: pytest.fail("Invalid export must not start"))
    with pytest.raises(SystemExit) as caught:
        cli.main(arguments + options)
    assert caught.value.code == 2
    assert capsys.readouterr().out == ""
    assert not path.exists() and not output.exists()


@pytest.mark.parametrize("failure", ["missing_database", "unknown_run", "invalid_run_id", "invalid_database"])
def test_export_errors_return_a_failed_receipt_without_creating_output(
        tmp_path, site, monkeypatch, capsys, failure):
    path = tmp_path / "run.sqlite"
    if failure in {"unknown_run", "invalid_run_id"}:
        assert cli.main(_arguments(path)) == 0
        capsys.readouterr()
    elif failure == "invalid_database":
        path.write_bytes(b"Not a SQLite database")
    before = path.read_bytes() if path.exists() else None
    run_id = {"unknown_run": "unknown", "invalid_run_id": " run "}.get(failure, "run")
    output = tmp_path / "exports" / "catalog.json"
    monkeypatch.setattr(cli, "run_catalog", lambda *args, **kwargs: pytest.fail("Export must not collect"))
    assert cli.main(_export_arguments(path, output, run_id)) == 1
    receipt = json.loads(capsys.readouterr().out)
    assert receipt["run_id"] == run_id
    assert receipt["catalog_export"]["status"] == "failed"
    assert receipt["catalog_export"]["output_path"] == str(output)
    assert receipt["catalog_export"]["error_type"]
    assert not output.parent.exists()
    if before is None:
        assert not path.exists()
    else:
        assert path.read_bytes() == before


@pytest.mark.parametrize("failure", ["output_directory", "protected_database", "unexpected_exception"])
def test_failed_export_keeps_the_collection_report_and_can_be_retried_without_recrawling(
        tmp_path, site, monkeypatch, capsys, caplog, failure):
    path = tmp_path / "run.sqlite"
    output = path if failure == "protected_database" else tmp_path / "catalog.json"
    if failure == "output_directory":
        output.mkdir()

    def fail_export(*args):
        raise AssertionError("Private export diagnostic details")

    with monkeypatch.context() as patch:
        if failure == "unexpected_exception":
            patch.setattr(cli, "export_catalog", fail_export)
        assert cli.main(_arguments(path) + ["--output", str(output)]) == 1
    report = json.loads(capsys.readouterr().out)
    assert report["status"] == "complete" and report["accepted_items"] == 4
    assert report["catalog_export"]["status"] == "failed"
    assert report["catalog_export"]["error_type"]
    assert "Catalog export failed run_id=run" in caplog.text
    assert "Private export diagnostic details" not in caplog.text
    with Storage(path) as store:
        assert store.get_run("run")["status"] == "complete"
    before = path.read_bytes()
    calls = len(site.requests)
    monkeypatch.setattr(cli, "run_catalog", lambda *args, **kwargs: pytest.fail("Retry must not collect"))
    retry = tmp_path / "retried.json"
    assert cli.main(_export_arguments(path, retry)) == 0
    assert json.loads(capsys.readouterr().out)["catalog_export"]["status"] == "succeeded"
    catalog = json.loads(retry.read_bytes())
    assert catalog["run"]["status"] == "complete" and len(catalog["products"]) == 4
    assert len(site.requests) == calls
    assert path.read_bytes() == before


@pytest.mark.parametrize("command", ["run", "export"])
def test_export_interruption_directs_retries_to_export_while_preserving_completed_data(
        tmp_path, site, monkeypatch, capsys, caplog, command):
    path, output = tmp_path / "run.sqlite", tmp_path / "catalog.json"
    if command == "export":
        assert cli.main(_arguments(path)) == 0
        capsys.readouterr()
    caplog.clear()

    def interrupt(*args):
        raise KeyboardInterrupt()

    monkeypatch.setattr(cli, "export_catalog", interrupt)
    arguments = (_export_arguments(path, output) if command == "export"
                 else _arguments(path) + ["--output", str(output)])
    assert cli.main(arguments) == 130
    assert capsys.readouterr().out == ""
    assert not output.exists()
    assert "phase=export" in caplog.text
    assert "retry export with the same database and run ID" in caplog.text
    assert "use resume" not in caplog.text
    with Storage(path) as store:
        assert store.get_run("run")["status"] == "complete"
        assert sum(len(store.list_items(task["task_id"])) for task in store.list_tasks("run")) == 4


_MODULE_WORKER = """
import runpy
import sys
import httpx
import pytest
import safco_catalog_scraper.runner as runner
from test_runner import PRODUCT_URLS, _Clock, _FixtureSite, _product_html, _set_clock

clock = _Clock()
patch = pytest.MonkeyPatch()
_set_clock(patch, clock)
site = _FixtureSite(clock)
mode = sys.argv[1]
if mode == 'partial':
    site.products[PRODUCT_URLS[0]] = _product_html('000000', name='Unfamiliar item', description='Assorted')
elif mode == 'failed':
    site.products[PRODUCT_URLS[0]] = b'<h1>Missing dataset</h1>'
collect = runner.run_catalog
def offline(*args, **kwargs):
    if mode == 'no_collection':
        raise AssertionError('Standalone export must not collect pages')
    return collect(*args, **kwargs, transport=httpx.MockTransport(site))
runner.run_catalog = offline
sys.argv = ['safco_catalog_scraper', *sys.argv[2:]]
runpy.run_module('safco_catalog_scraper', run_name='__main__')
"""


def _module_process(tmp_path, arguments, *, mode=None):
    root = Path(__file__).resolve().parents[1]
    environment = dict(os.environ, PYTHONDONTWRITEBYTECODE="1",
                       PYTHONPATH=os.pathsep.join((str(root / "src"), str(root / "tests"))))
    command = ([sys.executable, "-B", "-m", "safco_catalog_scraper"] if mode is None
               else [sys.executable, "-B", "-c", _MODULE_WORKER, mode])
    return subprocess.run(command + arguments, env=environment, cwd=tmp_path,
                          capture_output=True, text=True, timeout=15)


@pytest.mark.parametrize("mode,level,exit_code,accepted", [
    ("complete", "INFO", 0, 4), ("complete", "DEBUG", 0, 4),
    ("partial", "INFO", 1, 1), ("failed", "INFO", 1, 0),
])
def test_module_entry_point_separates_report_and_logs_and_sets_the_exit_code(
        tmp_path, mode, level, exit_code, accepted):
    output = tmp_path / "catalog.json"
    outcome = _module_process(tmp_path, _arguments(tmp_path / "run.sqlite", limit=1)
                              + ["--log-level", level, "--output", str(output)], mode=mode)
    assert outcome.returncode == exit_code, outcome.stderr
    report = json.loads(outcome.stdout)
    assert report["status"] == mode
    assert report["accepted_items"] == accepted
    assert report["catalog_export"] == {"status": "succeeded", "output_path": str(output.resolve())}
    catalog = json.loads(output.read_bytes())
    assert catalog["run"]["status"] == mode
    assert sum(len(product["items"]) for product in catalog["products"]) == accepted
    assert "Run progress run_id=run" in outcome.stderr
    assert "Run finished run_id=run status=" + mode in outcome.stderr
    assert "Catalog exported run_id=run" in outcome.stderr
    assert "fixture-public-search-key" not in outcome.stderr
    assert "Private fixture source wording" not in outcome.stderr
    assert "httpx:" not in outcome.stderr and "httpcore:" not in outcome.stderr
    assert "Traceback" not in outcome.stderr


def test_module_log_level_can_suppress_progress_without_changing_the_report(tmp_path):
    outcome = _module_process(tmp_path, _arguments(tmp_path / "run.sqlite")
                              + ["--log-level", "ERROR"], mode="complete")
    assert outcome.returncode == 0
    assert outcome.stderr == ""
    assert json.loads(outcome.stdout)["accepted_items"] == 4


def test_module_export_prints_only_a_receipt_and_writes_the_catalog_separately(tmp_path, site, capsys):
    path, output = tmp_path / "run.sqlite", tmp_path / "catalog.json"
    assert cli.main(_arguments(path)) == 0
    capsys.readouterr()
    before = path.read_bytes()
    outcome = _module_process(tmp_path, _export_arguments(path, output), mode="no_collection")
    assert outcome.returncode == 0, outcome.stderr
    assert json.loads(outcome.stdout) == {
        "run_id": "run", "catalog_export": {"status": "succeeded", "output_path": str(output.resolve())},
    }
    assert len(json.loads(output.read_bytes())["products"]) == 4
    assert "Catalog exported run_id=run status=complete products=4 items=4" in outcome.stderr
    assert "HTTP fetch" not in outcome.stderr
    assert "fixture-public-search-key" not in outcome.stderr
    assert "Private fixture source wording" not in outcome.stderr
    assert path.read_bytes() == before


@pytest.mark.parametrize("arguments", [["--help"], ["run", "--help"], ["resume", "--help"],
                                       ["recover", "--help"], ["export", "--help"]])
def test_module_help_is_available_without_starting_a_run(tmp_path, arguments):
    outcome = _module_process(tmp_path, arguments)
    assert outcome.returncode == 0
    assert outcome.stderr == ""
    assert "usage:" in outcome.stdout
    if arguments == ["--help"]:
        text = " ".join(outcome.stdout.split())
        assert "{run,resume,recover,export}" in text
        assert "Optional hosted recovery" in text
        assert "export prints a JSON receipt" in text
    else:
        assert "--output" in outcome.stdout
    assert list(tmp_path.iterdir()) == []
