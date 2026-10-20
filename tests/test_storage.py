"""Storage integration tests using real SQLite files and isolated crash workers."""

import hashlib
import json
import logging
import os
import signal
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from safco_catalog_scraper.storage import Storage, TaskSpec

CONFIG = {"categories": ["gloves", "sutures"], "limits": {"unresolved": 3}}
CODE_VERSION = "test-revision"
PRODUCT_URL = "https://example.test/product/a"


@pytest.mark.parametrize("expected,decoded", [(None, None), (0, 0), (36, 34)])
def test_failure_counts_and_replay_are_atomic(store, expected, decoded):
    task = store.enqueue_task("run", _product_spec())
    event = store.start_event(task, "extract")
    values = dict(expected_item_count=expected, decoded_item_count=decoded,
                  issues=[{"code": "fixture_issue"}])
    assert store.fail_product(event, **values) == "failed"
    saved = store.get_task(task)
    assert (saved["expected_item_count"], saved["decoded_item_count"]) == (expected, decoded)
    before = store.get_event(event), saved
    assert store.fail_product(event, **values) == "failed"
    assert (store.get_event(event), store.get_task(task)) == before
    with pytest.raises(ValueError, match="different result"):
        store.fail_product(event, **(values | {"decoded_item_count": 99}))


def test_failed_finalization_preserves_accepted_rows(store):
    task = store.enqueue_task("run", _product_spec())
    _accept_product(store, task)
    before = store.list_items(task)
    store.set_task_state(task, "pending")
    event = store.start_event(task, "extract")
    assert store.fail_product(event, expected_item_count=2, decoded_item_count=0) == "partial"
    assert store.list_items(task) == before
    assert store.get_event(event)["details"]["retained_item_count"] == 2


def test_failed_finalization_rolls_back_with_task_update(store):
    task = store.enqueue_task("run", _product_spec())
    event = store.start_event(task, "extract")
    before = store.get_event(event), store.get_task(task)
    store._db.execute("CREATE TEMP TRIGGER reject_failure BEFORE UPDATE ON tasks "
                      "WHEN NEW.status = 'failed' BEGIN SELECT RAISE(ABORT, 'fixture'); END")
    with pytest.raises(sqlite3.IntegrityError):
        store.fail_product(event, expected_item_count=0, decoded_item_count=0)
    assert (store.get_event(event), store.get_task(task)) == before


def test_event_metadata_omits_body_and_selected_evidence_remains_available(store):
    task = store.enqueue_task("run", _product_spec())
    event = store.start_event(task, "fetch")
    store.update_event(event, "succeeded", body=b"saved evidence")
    metadata, = store.list_events(task, include_bodies=False)
    assert "body" not in metadata
    assert metadata["content_hash"] == hashlib.sha256(b"saved evidence").hexdigest()
    assert store.get_event(event)["body"] == b"saved evidence"


@pytest.fixture
def store(tmp_path):
    """Provide an exclusively owned database containing one unfinished run."""
    with Storage(tmp_path / "run.sqlite") as storage:
        storage.create_run("run", CONFIG, CODE_VERSION)
        yield storage


def _product_spec(url=PRODUCT_URL, category="gloves"):
    return TaskSpec("product", url, {"url": url}, ({"category": category},))


def _items(price="16.49"):
    return [
        {
            "product_name": "Fixture gloves",
            "product_url": PRODUCT_URL,
            "captured_at": "2026-10-19T14:00:00Z",
            "sku": sku,
            "item_name": "Fixture gloves " + size,
            "manufacturer_part_number": "00" + sku,
            "item_description": size + ", 200/box",
            "selling_unit": "box",
            "pack_size": "200/box",
            "currency": "USD",
            "base_price": price,
            "quantity_tiers": [{"min_quantity": 10, "unit_price": "15.99"}],
            "notes": [],
        }
        for sku, size in (("0001", "small"), ("0002", "medium"))
    ]


def _missing_manufacturer_note():
    return {"field": "manufacturer", "reason": "source_absent",
            "source_field": "masterData.manufacturer_name", "source_text": None}


def _accept_product(store, task_id, items=None):
    rows = _items() if items is None else items
    event_id = store.start_event(task_id, "extract")
    store.commit_product(event_id, rows, expected_item_count=len(rows),
                         decoded_item_count=len(rows))
    return event_id


def _catalog_values(rows):
    return [{key: value for key, value in row.items()
             if key not in {"item_id", "extraction_event_id"}} for row in rows]


def test_run_configuration_survives_reopening(store):
    path = store.path
    before = store.get_run("run")
    store.close()
    with Storage(path) as reopened:
        assert reopened.get_run("run") == before
        assert before["config"] == CONFIG
        assert before["code_version"] == CODE_VERSION
        with pytest.raises(ValueError, match="already contains"):
            reopened.create_run("another-run", {}, CODE_VERSION)


def test_unknown_run_cannot_own_tasks(store):
    with pytest.raises(sqlite3.IntegrityError):
        store.enqueue_task("unknown-run", _product_spec())
    assert store.list_tasks("run") == []


def test_incompatible_catalog_schema_is_rejected_without_changing_saved_data(tmp_path):
    path = tmp_path / "legacy.sqlite"
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE catalog_items (item_id INTEGER PRIMARY KEY, "
                   "selling_unit TEXT, pack_size TEXT)")
        db.execute("INSERT INTO catalog_items VALUES (1, 'box', '200/box')")
    with pytest.raises(ValueError, match="Incompatible catalog schema"):
        Storage(path)
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT * FROM catalog_items").fetchall() == [(1, "box", "200/box")]
        assert db.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall() == [
            ("catalog_items",),
        ]


def test_discovery_replay_deduplicates_targets_and_preserves_progress(store):
    page = store.enqueue_task("run", TaskSpec("discovery", "gloves:0", {"page": 0}))
    event = store.start_event(page, "extract")
    targets = [_product_spec(), _product_spec(),
               TaskSpec("discovery", "gloves:1", {"page": 1})]

    ids = store.commit_discovery(event, targets, checkpoint={"next_page": 1})
    assert store.commit_discovery(event, targets, checkpoint={"next_page": 1}) == ids
    assert len(ids) == 2
    assert len(store.list_tasks("run")) == 3
    assert store.get_task(page)["status"] == "complete"
    assert store.get_task(page)["checkpoint"]["next_page"] == 1

    product = ids[0]
    fetch = store.start_event(product, "fetch")
    store.update_event(fetch, "succeeded", body=b"saved product response")
    before = store.get_task(product)
    assert store.enqueue_task("run", _product_spec(category="sutures")) == product
    after = store.get_task(product)
    assert after["discovered_from"] == [{"category": "gloves"}, {"category": "sutures"}]
    assert after["status"] == before["status"]
    assert after["checkpoint"] == before["checkpoint"]
    assert after["attempt_count"] == before["attempt_count"]


def test_conflicting_request_does_not_change_existing_task(store):
    task = store.enqueue_task("run", _product_spec())
    before = store.get_task(task)
    with pytest.raises(ValueError, match="different request"):
        store.enqueue_task("run", TaskSpec("product", PRODUCT_URL, {"url": "changed"}))
    assert store.get_task(task) == before


def test_source_evidence_and_checkpoint_survive_reopening(store):
    task = store.enqueue_task("run", _product_spec())
    event = store.start_event(task, "fetch")
    body = b"<html>fixed source evidence</html>"
    store.update_event(event, "succeeded", body=body, details={"http_status": 200},
                       checkpoint={"page": 0, "source_event_id": event})
    path = store.path
    store.close()

    with Storage(path) as reopened:
        saved = reopened.list_events(task)[0]
        assert saved["body"] == body
        assert saved["content_hash"] == hashlib.sha256(body).hexdigest()
        assert saved["details"]["http_status"] == 200
        assert reopened.get_task(task)["checkpoint"] == {"page": 0, "source_event_id": event}


def test_failed_budget_update_rolls_back_evidence_and_checkpoint(store):
    task = store.enqueue_task("run", _product_spec())
    event = store.start_event(task, "fetch")
    with pytest.raises(ValueError):
        store.update_event(event, "succeeded", body=b"must not be committed",
                           checkpoint={"page": 1}, budget={"reserved_usd": float("nan")})
    saved = store.list_events(task)[0]
    assert saved["status"] == "started"
    assert saved["body"] is None
    assert store.get_task(task)["checkpoint"] == {}
    assert store.get_run("run")["budget"] == {}


def test_failed_reservation_does_not_start_an_attempt(store):
    task = store.enqueue_task("run", _product_spec())
    with pytest.raises(ValueError):
        store.start_event(task, "recovery", budget={"reserved_usd": float("nan")})
    assert store.list_events(task) == []
    assert store.get_task(task)["attempt_count"] == 0
    assert store.get_task(task)["status"] == "pending"


@pytest.mark.parametrize("price", [None, "0.00", "16.49"])
def test_product_round_trip_and_replay_preserve_values(store, price):
    task = store.enqueue_task("run", _product_spec())
    items = _items(price)
    event = _accept_product(store, task, items)
    before = store.list_items(task)
    store.commit_product(event, items, expected_item_count=2, decoded_item_count=2)
    assert store.list_items(task) == before
    assert [row["sku"] for row in before] == ["0001", "0002"]
    assert before[0]["manufacturer_part_number"] == "000001"
    assert before[0]["base_price"] == price
    assert before[0]["item_description"] == "small, 200/box"
    assert before[0]["quantity_tiers"] == items[0]["quantity_tiers"]
    assert before[0]["manufacturer"] is None
    assert store.get_task(task)["status"] == "complete"


@pytest.mark.parametrize("unit,pack", [("box", "100/bx"),
                                      ("unfamiliar-unit", "3 pairs/unfamiliar-unit")])
def test_unit_pack_and_final_notes_survive_reopening_and_replay(store, unit, pack):
    task = store.enqueue_task("run", _product_spec())
    items = _items()
    items[0].update(selling_unit=unit, pack_size=pack, notes=[_missing_manufacturer_note()])
    event = _accept_product(store, task, items)
    before = store.list_items(task)
    path = store.path
    store.close()

    with Storage(path) as reopened:
        reopened.resume_run("run", CONFIG, CODE_VERSION)
        assert reopened.list_items(task) == before
        assert reopened.commit_product(event, items, expected_item_count=2,
                                       decoded_item_count=2) == 2
        saved = reopened.list_items(task)
        assert saved == before
        assert saved[0]["selling_unit"] == unit
        assert saved[0]["pack_size"] == pack
        assert saved[0]["notes"] == [_missing_manufacturer_note()]
        assert "notes_json" not in saved[0]
        assert reopened.get_task(task)["status"] == "complete"


def test_omitted_unit_pack_and_notes_use_storage_defaults(store):
    task = store.enqueue_task("run", _product_spec())
    items = _items()
    for item in items:
        for field in ("selling_unit", "pack_size", "notes"):
            item.pop(field)
    _accept_product(store, task, items)
    for row in store.list_items(task):
        assert row["selling_unit"] is None
        assert row["pack_size"] is None
        assert row["notes"] == []


def test_final_notes_persist_with_their_recovery_evidence(store):
    task = store.enqueue_task("run", _product_spec())
    fetch = store.start_event(task, "fetch")
    store.update_event(fetch, "succeeded", body=b"fixed source with ambiguous pack wording")
    issues = [{"code": "unparsed_pack_size", "field": "pack_size", "sku": "0001"}]
    initial = store.start_event(task, "extract")
    store.update_event(initial, "failed", details={"issues": issues})
    budget = {"recovery_cases": 1, "spent_usd": "0.01"}
    recovery = store.start_event(task, "recovery", details={"input_event_ids": [fetch]},
                                 budget=budget)
    steps = [{"action": "inspect_saved_source", "status": "succeeded",
              "observation": "Selling unit established; pack size remains ambiguous"}]
    store.update_event(recovery, "succeeded", details={"outcome": "partial", "steps": steps})
    assert store.list_items(task) == []

    items = _items()
    note = {"field": "pack_size", "reason": "unparsed_source",
            "source_field": "masterData.description",
            "source_text": "Assorted sizes; quantities vary × 2"}
    items[0].update(pack_size=None, item_description=note["source_text"], notes=[note])
    final = store.start_event(task, "extract",
                              details={"input_event_ids": [fetch, recovery]})
    store.commit_product(final, items, status="partial", expected_item_count=2,
                         decoded_item_count=2, issues=issues)
    before = store.list_items(task)
    path = store.path
    store.close()

    with Storage(path) as reopened:
        reopened.resume_run("run", CONFIG, CODE_VERSION)
        saved = reopened.list_items(task)
        events = {event["event_id"]: event for event in reopened.list_events(task)}
        assert saved == before
        assert saved[0]["selling_unit"] == "box"
        assert saved[0]["pack_size"] is None
        assert saved[0]["notes"] == [note]
        assert saved[0]["extraction_event_id"] == final
        assert events[final]["details"]["input_event_ids"] == [fetch, recovery]
        assert events[recovery]["details"]["outcome"] == "partial"
        assert events[recovery]["details"]["steps"] == steps
        assert events[fetch]["body"] == b"fixed source with ambiguous pack wording"
        assert reopened.get_task(task)["status"] == "partial"
        assert reopened.get_task(task)["issues"] == issues
        assert reopened.get_run("run")["budget"] == budget


@pytest.mark.parametrize("field,value", [
    ("selling_unit", "different-unit"), ("pack_size", "different pack wording"),
    ("notes", [_missing_manufacturer_note()]),
])
def test_committed_unit_pack_or_notes_cannot_change_on_replay(store, field, value):
    task = store.enqueue_task("run", _product_spec())
    event = _accept_product(store, task)
    before = store.list_items(task), store.get_task(task), store.list_events(task)
    changed = _items()
    changed[0][field] = value
    with pytest.raises(ValueError, match="different result"):
        store.commit_product(event, changed, expected_item_count=2, decoded_item_count=2)
    assert (store.list_items(task), store.get_task(task), store.list_events(task)) == before


@pytest.mark.parametrize("notes,error", [
    (None, sqlite3.IntegrityError), ({}, sqlite3.IntegrityError),
    ("not an array", sqlite3.IntegrityError),
    ([{"source_text": float("nan")}], ValueError),
])
def test_invalid_notes_preserve_previous_records_and_unfinished_event(store, notes, error):
    task = store.enqueue_task("run", _product_spec())
    original = _items()
    original[0]["notes"] = [_missing_manufacturer_note()]
    _accept_product(store, task, original)
    before = store.list_items(task)
    store.set_task_state(task, "pending")
    event = store.start_event(task, "extract")
    replacement = _items("18.00")
    replacement[1]["notes"] = notes
    with pytest.raises(error):
        store.commit_product(event, replacement, expected_item_count=2, decoded_item_count=2)
    assert store.list_items(task) == before
    assert store.get_task(task)["status"] == "running"
    assert store.list_events(task)[-1]["status"] == "started"


def test_committed_operation_rejects_a_changed_payload(store):
    task = store.enqueue_task("run", _product_spec())
    event = _accept_product(store, task)
    before = store.list_items(task)
    with pytest.raises(ValueError, match="different result"):
        store.commit_product(event, _items("18.00"), expected_item_count=2,
                             decoded_item_count=2)
    assert store.list_items(task) == before


@pytest.mark.parametrize("invalid", ["duplicate_sku", "missing_name", "invalid_paths"])
def test_invalid_replacement_preserves_previous_catalog_and_event(store, invalid):
    task = store.enqueue_task("run", _product_spec())
    _accept_product(store, task)
    before = store.list_items(task)
    store.set_task_state(task, "pending")
    event = store.start_event(task, "extract")
    replacements = _items("18.00")
    if invalid == "duplicate_sku":
        replacements[1]["sku"] = replacements[0]["sku"]
    elif invalid == "missing_name":
        replacements[1]["item_name"] = None
    else:
        replacements[1]["category_paths"] = {"invalid": "not an array"}

    with pytest.raises(sqlite3.IntegrityError):
        store.commit_product(event, replacements, expected_item_count=2,
                             decoded_item_count=2)
    assert store.list_items(task) == before
    assert store.get_task(task)["status"] == "running"
    assert store.list_events(task)[-1]["status"] == "started"


def test_partial_and_failed_states_reflect_accepted_records(store):
    task = store.enqueue_task("run", _product_spec())
    with pytest.raises(ValueError, match="require accepted"):
        store.set_task_state(task, "partial")
    event = store.start_event(task, "extract")
    issues = [{"code": "missing_items", "expected": 2, "accepted": 1}]
    store.commit_product(event, _items()[:1], status="partial", expected_item_count=2,
                         decoded_item_count=1, issues=issues)
    assert store.get_task(task)["status"] == "partial"
    assert store.get_task(task)["issues"] == issues
    with pytest.raises(ValueError, match="must be partial"):
        store.set_task_state(task, "failed")


@pytest.mark.parametrize("config, revision", [({"changed": True}, CODE_VERSION),
                                             (CONFIG, "different-revision")])
def test_incompatible_resume_changes_nothing(store, config, revision):
    task = store.enqueue_task("run", _product_spec())
    store.start_event(task, "fetch")
    before = store.get_run("run"), store.get_task(task), store.list_events(task)
    with pytest.raises(ValueError, match="original code and configuration"):
        store.resume_run("run", config, revision)
    assert (store.get_run("run"), store.get_task(task), store.list_events(task)) == before


@pytest.mark.parametrize("status", ["complete", "partial", "failed"])
def test_terminal_run_is_not_automatically_resumed(store, status):
    store.finish_run("run", status, reason="fixture outcome")
    before = store.get_run("run")
    with pytest.raises(ValueError, match="terminal"):
        store.resume_run("run", CONFIG, CODE_VERSION)
    assert store.get_run("run") == before


@pytest.fixture
def recovery_ready(tmp_path):
    config = CONFIG | {"recovery": {"mode": "deferred", "reason": "Not configured"},
                       "recovery_endpoint": None}
    enabled = config | {"recovery": {"mode": "hosted", "model": "fixture-model"},
                        "recovery_endpoint": "https://recovery.example/api/recovery"}
    with Storage(tmp_path / "recovery.sqlite") as store:
        store.create_run("run", config, CODE_VERSION, budget={"reserved_cases": 0, "consumed_cases": 0})
        task = store.enqueue_task("run", _product_spec())
        fetched = store.start_event(task, "fetch")
        store.update_event(fetched, "succeeded", body=b"{}", details={"http_status": 200, "body_complete": True},
                           checkpoint={"source_event_id": fetched})
        _accept_product(store, task)
        store.set_task_state(task, "partial", issues=[{"code": "unresolved_pack_size", "sku": "0001"}])
        store.finish_run("run", "partial", reason="unresolved_product_limit_reached")
        yield store, task, enabled


def test_enabling_recovery_requeues_saved_partial_work_without_resetting_history(recovery_ready):
    store, task, enabled = recovery_ready
    old_run, old_task = store.get_run("run"), store.get_task(task)
    old_items, old_events = store.list_items(task), store.list_events(task)
    assert store.enable_recovery("run", enabled, "recovery-revision") == 1
    run, retried = store.get_run("run"), store.get_task(task)
    assert run["config"] == enabled and run["code_version"] == "recovery-revision"
    assert run["status"] == "running" and run["finished_at"] is None and run["stop_reason"] is None
    assert run["budget"] == old_run["budget"]
    assert retried["status"] == "pending" and retried["stage"] == "extract"
    assert retried["attempt_count"] == old_task["attempt_count"]
    assert retried["checkpoint"] == old_task["checkpoint"]
    assert store.list_items(task) == old_items and store.list_events(task)[:-1] == old_events
    audit = store.list_events(task)[-1]
    assert audit["details"]["operation"] == "enable_recovery"
    assert audit["details"]["previous_config_hash"] == old_run["config_hash"]
    assert audit["details"]["previous_code_version"] == CODE_VERSION
    with pytest.raises(ValueError):
        store.enable_recovery("run", enabled, "recovery-revision")


def test_enabling_recovery_rolls_back_configuration_tasks_and_audit_together(recovery_ready):
    store, task, enabled = recovery_ready
    before = store.get_run("run"), store.get_task(task), store.list_events(task), store.list_items(task)
    store._db.execute("CREATE TEMP TRIGGER reject_recovery BEFORE UPDATE ON tasks "
                      "BEGIN SELECT RAISE(ABORT, 'fixture'); END")
    with pytest.raises(sqlite3.IntegrityError):
        store.enable_recovery("run", enabled, "recovery-revision")
    assert (store.get_run("run"), store.get_task(task), store.list_events(task), store.list_items(task)) == before


def test_enabling_recovery_cannot_change_collection_settings(recovery_ready):
    store, task, enabled = recovery_ready
    before = store.get_run("run"), store.get_task(task), store.list_events(task)
    with pytest.raises(ValueError, match="collection settings"):
        store.enable_recovery("run", enabled | {"categories": ["different"]}, "recovery-revision")
    assert (store.get_run("run"), store.get_task(task), store.list_events(task)) == before


def test_explicit_interruption_clears_stop_metadata_on_resume(store):
    store.finish_run("run", "interrupted", reason="fixture interruption")
    reordered_config = {"limits": CONFIG["limits"], "categories": CONFIG["categories"]}
    resumed = store.resume_run("run", reordered_config, CODE_VERSION)
    assert resumed["status"] == "running"
    assert resumed["stop_reason"] is None
    assert resumed["finished_at"] is None


def test_older_attempt_cannot_update_or_accept_work(store):
    task = store.enqueue_task("run", _product_spec())
    old_event = store.start_event(task, "extract")
    store.set_task_state(task, "pending")
    store.start_event(task, "extract")
    with pytest.raises(ValueError, match="current running attempt"):
        store.commit_product(old_event, _items())
    with pytest.raises(ValueError, match="older task attempt"):
        store.update_event(old_event, "failed")
    assert store.list_items(task) == []


def test_checkpoint_and_warning_logs_include_context(store, caplog):
    with caplog.at_level(logging.INFO, logger="safco_catalog_scraper.storage"):
        task = store.enqueue_task("run", _product_spec())
        items = _items()
        note = {"field": "selling_unit", "reason": "normalized_source_wording",
                "source_field": "masterData.description", "source_text": "small, 200/bx"}
        items[0]["notes"] = [note]
        _accept_product(store, task, items)
        store.set_task_state(task, "pending")
        event = store.start_event(task, "fetch")
        store.update_event(event, "failed", details={"error": "fixture timeout"})
    records = [r for r in caplog.records if r.name == "safco_catalog_scraper.storage"]
    assert any(r.levelno == logging.INFO and "run_id=run" in r.getMessage()
               and f"task_id={task}" in r.getMessage() and "accepted=2" in r.getMessage()
               and "notes=1" in r.getMessage()
               for r in records)
    assert any(r.levelno == logging.WARNING and f"event_id={event}" in r.getMessage()
               and "status=failed" in r.getMessage() for r in records)
    assert note["source_text"] not in caplog.text


# SQLite triggers interrupt real writes without adding fault hooks to production code.
WORKER = r"""
import json
import os
import signal
import sys
from safco_catalog_scraper.storage import Storage, TaskSpec

path, task_id, operation, payload = sys.argv[1:]
task_id = int(task_id)
payload = json.loads(payload)
if operation == "ownership":
    try:
        with Storage(path):
            pass
    except RuntimeError as error:
        if "Another collector owns" in str(error):
            sys.exit(17)
        raise
    sys.exit(1)

with Storage(path) as store:
    if operation == "recovery":
        event = store.start_event(task_id, "recovery", budget=payload["budget"])
        store.update_event(event, "started", details={"steps": payload["steps"]},
                           checkpoint={"recovery_event_id": event, "step": 1})
    elif operation == "discovery":
        event = store.start_event(task_id, "extract")
        store._db.create_function("crash_now", 0,
                                  lambda: os.kill(os.getpid(), signal.SIGKILL))
        store._db.execute("CREATE TRIGGER crash_insert BEFORE INSERT ON tasks "
                          "WHEN NEW.target_key = 'https://example.test/product/b' "
                          "BEGIN SELECT crash_now(); END")
        store.commit_discovery(event, [TaskSpec("product", url, {"url": url})
                                       for url in payload["urls"]])
    else:
        store.set_task_state(task_id, "pending")
        event = store.start_event(task_id, "extract")
        if operation == "before_commit":
            store._db.create_function("crash_now", 0,
                                      lambda: os.kill(os.getpid(), signal.SIGKILL))
            store._db.execute("CREATE TRIGGER crash_insert BEFORE INSERT ON catalog_items "
                              "WHEN NEW.sku = '0002' BEGIN SELECT crash_now(); END")
        store.commit_product(event, payload["items"], expected_item_count=2,
                             decoded_item_count=2)
    os.kill(os.getpid(), signal.SIGKILL)
"""


def _run_worker(path, task_id, operation, payload=None):
    env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1] / "src"),
               PYTHONDONTWRITEBYTECODE="1")
    result = subprocess.run(
        [sys.executable, "-B", "-c", WORKER, str(path), str(task_id), operation,
         json.dumps(payload or {})],
        env=env, capture_output=True, text=True, timeout=15,
    )
    expected = 17 if operation == "ownership" else -signal.SIGKILL
    assert result.returncode == expected, result.stderr


def test_second_process_cannot_own_an_active_database(store):
    _run_worker(store.path, 0, "ownership")


@pytest.mark.parametrize("phase", ["before_commit", "after_commit"])
def test_product_commit_survives_abrupt_process_death(store, phase):
    task = store.enqueue_task("run", _product_spec())
    _accept_product(store, task)
    baseline = store.list_items(task)
    path = store.path
    store.close()
    replacement = _items("18.00")
    for item in replacement:
        item.update(selling_unit="unfamiliar-unit", pack_size="3 pairs/unfamiliar-unit",
                    notes=[_missing_manufacturer_note()])
    _run_worker(path, task, phase, {"items": replacement})

    with Storage(path) as reopened:
        if phase == "before_commit":
            assert reopened.list_items(task) == baseline
            assert reopened.get_task(task)["status"] == "running"
            assert reopened.list_events(task)[-1]["status"] == "started"
            reopened._db.execute("DROP TRIGGER crash_insert")
            reopened.resume_run("run", CONFIG, CODE_VERSION)
            assert reopened.get_task(task)["status"] == "pending"
            assert reopened.list_events(task)[-1]["status"] == "uncertain"
            _accept_product(reopened, task, replacement)
        else:
            attempts = reopened.get_task(task)["attempt_count"]
            reopened.resume_run("run", CONFIG, CODE_VERSION)
            assert reopened.get_task(task)["attempt_count"] == attempts
        expected = [row | replacement_item for row, replacement_item in zip(baseline, replacement)]
        assert _catalog_values(reopened.list_items(task)) == _catalog_values(expected)
        assert reopened.get_task(task)["status"] == "complete"
        assert reopened.list_events(task)[-1]["status"] == "succeeded"


def test_discovery_crash_does_not_commit_a_partial_worklist(store):
    page = store.enqueue_task("run", TaskSpec("discovery", "gloves:0", {"page": 0}))
    fetch = store.start_event(page, "fetch")
    store.update_event(fetch, "succeeded", body=b"fixed category evidence")
    path = store.path
    store.close()
    urls = [PRODUCT_URL, "https://example.test/product/b"]
    _run_worker(path, page, "discovery", {"urls": urls})

    with Storage(path) as reopened:
        assert len(reopened.list_tasks("run")) == 1
        assert reopened.get_task(page)["status"] == "running"
        assert reopened.list_events(page)[0]["body"] == b"fixed category evidence"
        reopened._db.execute("DROP TRIGGER crash_insert")
        reopened.resume_run("run", CONFIG, CODE_VERSION)
        event = reopened.start_event(page, "extract")
        reopened.commit_discovery(event, [TaskSpec("product", url, {"url": url})
                                           for url in urls])
        assert len(reopened.list_tasks("run")) == 3
        assert reopened.get_task(page)["status"] == "complete"


def test_saved_recovery_observation_and_budget_survive_process_death(store):
    complete = store.enqueue_task("run", _product_spec())
    _accept_product(store, complete)
    baseline = store.list_items(complete)
    task = store.enqueue_task("run", _product_spec("https://example.test/product/b"))
    budget = {"recovery_cases": 1, "reserved_usd": "0.10", "spent_usd": "0.01"}
    steps = [{"action": "inspect_page", "status": "succeeded", "observation": "saved rows"}]
    path = store.path
    store.close()
    _run_worker(path, task, "recovery", {"budget": budget, "steps": steps})

    with Storage(path) as reopened:
        reopened.resume_run("run", CONFIG, CODE_VERSION)
        saved_task = reopened.get_task(task)
        saved_event = reopened.list_events(task)[0]
        assert saved_task["status"] == "pending"
        assert saved_task["stage"] == "recovery"
        assert saved_task["attempt_count"] == 1
        assert saved_task["checkpoint"] == {"recovery_event_id": saved_event["event_id"],
                                             "step": 1}
        assert saved_event["status"] == "uncertain"
        assert saved_event["details"]["steps"] == steps
        assert reopened.get_run("run")["budget"] == budget
        assert reopened.get_task(complete)["status"] == "complete"
        assert reopened.list_items(complete) == baseline
