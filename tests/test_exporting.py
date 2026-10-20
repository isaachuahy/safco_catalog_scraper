"""Offline snapshot contracts using committed SQLite data and real file replacement."""

import copy
import json
import logging
import os
import shutil
import signal
import sqlite3
import stat
import subprocess
import sys
from pathlib import Path

import pytest

import safco_catalog_scraper.exporting as exporting
from safco_catalog_scraper.exporting import export_catalog, read_catalog
from safco_catalog_scraper.storage import Storage
from test_runner import PRODUCT_URLS, _Clock, _FixtureSite, _catalog, _config, _product_html, _run, _set_clock


@pytest.fixture
def site(monkeypatch):
    clock = _Clock()
    _set_clock(monkeypatch, clock)
    return _FixtureSite(clock)


@pytest.fixture
def source(tmp_path, site):
    path = tmp_path / "run.sqlite"
    _run(path, site)
    return path


def _product_items(path, url=PRODUCT_URLS[0]):
    return [item for item in _catalog(path) if item["product_url"] == url]


def _replace_items(path, items, url=PRODUCT_URLS[0]):
    with Storage(path) as store:
        task = next(task for task in store.list_tasks("run") if task["target_key"] == url)
        store.set_task_state(task["task_id"], "pending")
        event = store.start_event(task["task_id"], "extract")
        store.commit_product(event, items, expected_item_count=len(items), decoded_item_count=len(items))


def _flatten(catalog):
    return [item | {"product_url": product["product_url"], "product_name": product["product_name"]}
            for product in catalog["products"] for item in product["items"]]


def test_snapshot_preserves_saved_metadata_and_all_catalog_values(source, tmp_path):
    before = source.read_bytes()
    catalog = read_catalog(source, "run")
    assert set(catalog) == {"run", "products"}
    assert len(catalog["products"]) == 4
    assert _flatten(catalog) == _catalog(source)
    with Storage(source) as store:
        assert catalog["run"] == store.get_run("run")
    item = catalog["products"][0]["items"][0]
    assert item["sku"] == "000000"
    assert item["manufacturer_part_number"] == "0012-A"
    assert item["base_price"] == "16.49"
    assert item["quantity_tiers"] == [{"min_quantity": 10, "unit_price": "15.99"}]
    assert item["pack_size"] == "100/bx" and item["selling_unit"] == "box"
    assert item["notes"]
    target = tmp_path / "exports" / "2026-10-20.json"
    assert export_catalog(source, "run", target) == target.resolve()
    payload = target.read_bytes()
    assert json.loads(payload) == catalog
    assert payload.endswith(b"\n")
    assert b"fixture-public-search-key" not in payload
    assert b"window.masterData" not in payload
    assert b"extraction_event_id" not in payload and b"item_id" not in payload
    assert "schema_version" not in catalog and "exported_at" not in catalog["run"]
    assert source.read_bytes() == before


def test_grouping_retains_item_identity_manufacturer_and_observation_fields(source):
    first, = _product_items(source)
    variation = first | {
        "sku": "000010", "item_name": "GLASSVAN fixture blade", "manufacturer": "Other source manufacturer",
        "captured_at": "2026-10-20T12:01:00+00:00", "category_paths": [["Other source breadcrumb"]],
    }
    _replace_items(source, [variation, first])
    product = read_catalog(source, "run")["products"][0]
    assert set(product) == {"product_url", "product_name", "items"}
    assert product["product_name"] == first["product_name"]
    assert [item["sku"] for item in product["items"]] == ["000000", "000010"]
    assert [item["item_name"] for item in product["items"]] == [first["item_name"], variation["item_name"]]
    assert [item["manufacturer"] for item in product["items"]] == [first["manufacturer"], variation["manufacturer"]]
    assert product["items"][1]["captured_at"] == variation["captured_at"]
    assert product["items"][1]["category_paths"] == variation["category_paths"]


def test_nullable_fields_empty_tiers_and_final_notes_are_not_dropped(source):
    item, = _product_items(source)
    nullable = ("category_paths", "item_description", "manufacturer", "manufacturer_part_number",
                "selling_unit", "pack_size", "availability", "currency", "base_price")
    item.update({field: None for field in nullable})
    item["quantity_tiers"] = []
    item["notes"] = [{"field": field, "reason": "unparsed_source", "source_field": "fixture." + field,
                      "source_text": "Unsupported source wording"} for field in nullable]
    _replace_items(source, [item])
    result = read_catalog(source, "run")["products"][0]["items"][0]
    assert all(field in result and result[field] is None for field in nullable)
    assert result["quantity_tiers"] == []
    assert result["notes"] == item["notes"]


def test_the_same_sku_on_distinct_product_urls_remains_separate(source):
    item, = _product_items(source, PRODUCT_URLS[1])
    item["sku"] = "000000"
    _replace_items(source, [item], PRODUCT_URLS[1])
    matches = [item for item in _flatten(read_catalog(source, "run")) if item["sku"] == "000000"]
    assert len(matches) == 2
    assert {item["product_url"] for item in matches} == set(PRODUCT_URLS[:2])


def test_repeated_export_is_byte_identical_despite_different_item_insertion_order(source, tmp_path):
    first, = _product_items(source)
    items = [first | {"sku": sku, "item_name": "Épreuve item " + sku} for sku in ("000010", "000002", "000001")]
    _replace_items(source, items)
    target = tmp_path / "catalog.json"
    export_catalog(source, "run", target)
    expected = target.read_bytes()
    _replace_items(source, list(reversed(items)))
    export_catalog(source, "run", target)
    assert target.read_bytes() == expected
    export_catalog(source, "run", target)
    assert target.read_bytes() == expected
    products = json.loads(expected)["products"]
    assert [product["product_url"] for product in products] == sorted(PRODUCT_URLS)
    assert [item["sku"] for item in products[0]["items"]] == ["000001", "000002", "000010"]
    assert "Épreuve".encode() in expected


@pytest.mark.parametrize("status,accepted", [("running", 4), ("interrupted", 1), ("partial", 1), ("failed", 0)])
def test_incomplete_runs_keep_their_status_and_progress_without_resumption(
        tmp_path, site, monkeypatch, status, accepted):
    path = tmp_path / "run.sqlite"
    if status == "partial":
        site.products[PRODUCT_URLS[0]] = _product_html("000000", name="Unfamiliar item", description="Assorted")
    elif status == "failed":
        site.products[PRODUCT_URLS[0]] = b"<h1>Missing dataset</h1>"
    elif status == "interrupted":
        def interrupt(attempt):
            raise KeyboardInterrupt()
        site.replies[PRODUCT_URLS[1]] = interrupt
    if status == "interrupted":
        with pytest.raises(KeyboardInterrupt):
            _run(path, site)
    else:
        with monkeypatch.context() as patch:
            if status == "running":
                # Model a process exit after all product commits but before run finalization.
                patch.setattr(Storage, "finish_run", lambda *args, **kwargs: None)
            _run(path, site, _config(unresolved_product_limit=1))
    with Storage(path) as store:
        if status == "interrupted":
            active = next(task for task in store.list_tasks("run") if task["status"] == "running")
            event = store.list_events(active["task_id"])[-1]
            store.update_event(event["event_id"], "started", budget={"reserved_cases": 2, "consumed_cases": 1})
        saved_run = store.get_run("run")
        tasks = store.list_tasks("run")
        events = [store.list_events(task["task_id"]) for task in tasks]
        before = path.read_bytes()
        target = tmp_path / "catalog.json"
        export_catalog(path, "run", target)
        catalog = json.loads(target.read_bytes())
        assert catalog["run"] == saved_run
        assert catalog["run"]["status"] == status
        if status == "interrupted":
            assert catalog["run"]["budget"] == {"reserved_cases": 2, "consumed_cases": 1}
        assert len(_flatten(catalog)) == accepted
        assert store.list_tasks("run") == tasks
        assert [store.list_events(task["task_id"]) for task in tasks] == events
        assert path.read_bytes() == before


@pytest.mark.parametrize("run_id", ["unknown", "' OR 1=1 --"])
def test_unknown_run_does_not_replace_output(source, tmp_path, run_id):
    target = tmp_path / "catalog.json"
    previous = b'{"previous": true}\n'
    target.write_bytes(previous)
    with pytest.raises(KeyError):
        export_catalog(source, run_id, target)
    assert target.read_bytes() == previous


@pytest.mark.parametrize("run_id", ["", " run", "run ", None, 7])
def test_invalid_run_id_is_rejected_before_creating_output(source, tmp_path, run_id):
    target = tmp_path / "exports" / "catalog.json"
    with pytest.raises(ValueError):
        export_catalog(source, run_id, target)
    assert not target.parent.exists()


def test_missing_database_is_not_created(tmp_path):
    source = tmp_path / "missing.sqlite"
    target = tmp_path / "exports" / "catalog.json"
    with pytest.raises(sqlite3.OperationalError):
        export_catalog(source, "run", target)
    assert not source.exists()
    assert not source.with_name(source.name + ".lock").exists()
    assert not target.parent.exists()


@pytest.mark.parametrize("kind", ["invalid_file", "missing_schema"])
def test_database_errors_leave_existing_output_intact(tmp_path, kind):
    source = tmp_path / "broken.sqlite"
    if kind == "invalid_file":
        source.write_bytes(b"Not a SQLite database")
    else:
        with sqlite3.connect(source) as database:
            database.execute("CREATE TABLE unrelated (value TEXT)")
    before = source.read_bytes()
    target = tmp_path / "catalog.json"
    previous = b'{"previous": true}\n'
    target.write_bytes(previous)
    with pytest.raises(sqlite3.Error):
        export_catalog(source, "run", target)
    assert target.read_bytes() == previous
    assert source.read_bytes() == before


def test_conflicting_family_names_do_not_silently_choose_one_or_replace_output(source, tmp_path):
    first, = _product_items(source)
    conflicting = first | {"sku": "000009", "product_name": "Different source family"}
    _replace_items(source, [first, conflicting])
    target = tmp_path / "catalog.json"
    previous = b'{"previous": true}\n'
    target.write_bytes(previous)
    with pytest.raises(ValueError, match="Conflicting product family names"):
        export_catalog(source, "run", target)
    assert target.read_bytes() == previous


@pytest.mark.parametrize("suffix", ["", ".lock", "-journal", "-wal", "-shm"])
def test_database_and_sidecar_paths_cannot_be_export_destinations(source, suffix):
    target = source.with_name(source.name + suffix)
    before = source.read_bytes()
    existed = target.exists()
    previous = target.read_bytes() if existed else None
    with pytest.raises(ValueError, match="cannot replace the database"):
        export_catalog(source, "run", target)
    assert target.exists() == existed
    if existed:
        assert target.read_bytes() == previous
    assert source.read_bytes() == before


@pytest.mark.parametrize("suffix", ["", ".lock"])
@pytest.mark.parametrize("kind", ["symlink", "hard_link"])
def test_file_aliases_cannot_bypass_source_protection(source, tmp_path, suffix, kind):
    protected = source.with_name(source.name + suffix)
    target = tmp_path / "alias.json"
    if kind == "symlink":
        target.symlink_to(protected)
    else:
        os.link(protected, target)
    previous = protected.read_bytes()
    with pytest.raises(ValueError, match="cannot replace the database"):
        export_catalog(source, "run", target)
    assert protected.read_bytes() == previous
    assert target.samefile(protected)
    if kind == "symlink":
        assert target.is_symlink()


def test_database_filename_is_encoded_as_a_uri(source, tmp_path):
    encoded = tmp_path / "source ?#100%.sqlite"
    shutil.copyfile(source, encoded)
    assert read_catalog(encoded, "run") == read_catalog(source, "run")


def test_export_ignores_uncommitted_database_changes(source, tmp_path):
    expected = read_catalog(source, "run")
    writer = sqlite3.connect(source)
    try:
        writer.execute("BEGIN IMMEDIATE")
        writer.execute("UPDATE runs SET status='partial'")
        writer.execute("UPDATE catalog_items SET base_price='42.00'")
        target = tmp_path / "catalog.json"
        export_catalog(source, "run", target)
        assert json.loads(target.read_bytes()) == expected
    finally:
        writer.rollback()
        writer.close()


def test_metadata_and_items_share_one_snapshot_when_a_competing_writer_commits(source, monkeypatch):
    expected = read_catalog(source, "run")
    writer = sqlite3.connect(source)
    # WAL lets the competing writer commit while the export's read transaction stays open.
    assert writer.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
    actual_connect = sqlite3.connect
    committed = []

    class SnapshotConnection(sqlite3.Connection):
        def execute(self, sql, *args, **kwargs):
            if "FROM catalog_items" in sql and not committed:
                writer.execute("UPDATE runs SET status='partial'")
                writer.execute("UPDATE catalog_items SET base_price='42.00'")
                writer.commit()
                committed.append(True)
            return super().execute(sql, *args, **kwargs)

    try:
        with monkeypatch.context() as patch:
            patch.setattr(exporting.sqlite3, "connect",
                          lambda *args, **kwargs: actual_connect(*args, factory=SnapshotConnection, **kwargs))
            assert read_catalog(source, "run") == expected
        assert committed == [True]
        current = read_catalog(source, "run")
        assert current["run"]["status"] == "partial"
        assert all(item["base_price"] == "42.00" for item in _flatten(current))
    finally:
        writer.close()


@pytest.mark.parametrize("phase", ["serialization", "file_sync", "replace", "directory_sync"])
def test_write_failures_propagate_and_clean_temporary_files_at_the_correct_boundary(
        source, tmp_path, monkeypatch, caplog, phase):
    target = tmp_path / "exports" / "catalog.json"
    target.parent.mkdir()
    previous = b'{"previous": true}\n'
    target.write_bytes(previous)
    expected = read_catalog(source, "run")
    before = source.read_bytes()
    with monkeypatch.context() as patch:
        if phase == "serialization":
            def fail_dump(value, stream, **kwargs):
                stream.write('{"incomplete": ')
                raise OSError("fixture serialization failure")
            patch.setattr(exporting.json, "dump", fail_dump)
        elif phase == "replace":
            def fail_replace(*args):
                raise OSError("fixture replacement failure")
            patch.setattr(exporting.os, "replace", fail_replace)
        else:
            actual_sync = os.fsync

            def fail_sync(descriptor):
                is_directory = stat.S_ISDIR(os.fstat(descriptor).st_mode)
                if is_directory == (phase == "directory_sync"):
                    raise OSError("fixture sync failure")
                return actual_sync(descriptor)

            patch.setattr(exporting.os, "fsync", fail_sync)
        with caplog.at_level(logging.INFO, logger="safco_catalog_scraper.exporting"):
            with pytest.raises(OSError, match="fixture"):
                export_catalog(source, "run", target)
    if phase == "directory_sync":
        assert json.loads(target.read_bytes()) == expected
    else:
        assert target.read_bytes() == previous
    assert list(target.parent.glob(".*.tmp")) == []
    assert source.read_bytes() == before
    assert "Catalog exported" not in caplog.text


@pytest.mark.parametrize("value", [float("nan"), float("inf")])
def test_nonfinite_values_cannot_publish_invalid_json(source, tmp_path, monkeypatch, value):
    invalid = copy.deepcopy(read_catalog(source, "run"))
    invalid["run"]["config"]["invalid_fixture_value"] = value
    monkeypatch.setattr(exporting, "read_catalog", lambda *args: invalid)
    target = tmp_path / "catalog.json"
    previous = b'{"previous": true}\n'
    target.write_bytes(previous)
    with pytest.raises(ValueError):
        export_catalog(source, "run", target)
    assert target.read_bytes() == previous
    assert list(tmp_path.glob(".*.tmp")) == []


def test_an_existing_directory_cannot_be_replaced_by_the_snapshot(source, tmp_path):
    target = tmp_path / "existing-directory"
    target.mkdir()
    sentinel = target / "keep.txt"
    sentinel.write_text("Keep this file", encoding="utf-8")
    with pytest.raises(OSError):
        export_catalog(source, "run", target)
    assert sentinel.read_text(encoding="utf-8") == "Keep this file"
    assert list(tmp_path.glob(".*.tmp")) == []


def test_success_log_surfaces_status_and_counts_without_item_wording_or_credentials(source, tmp_path, caplog):
    target = tmp_path / "catalog.json"
    with caplog.at_level(logging.INFO, logger="safco_catalog_scraper.exporting"):
        export_catalog(source, "run", target)
    assert "Catalog exported run_id=run status=complete products=4 items=4" in caplog.text
    assert str(target.resolve()) in caplog.text
    assert "fixture-public-search-key" not in caplog.text
    assert "Private fixture source wording" not in caplog.text


_CRASH_WORKER = """
import os
import signal
import sys
import safco_catalog_scraper.exporting as exporting

replace = os.replace
def crash_replace(source, destination):
    if sys.argv[3] == 'before':
        os.kill(os.getpid(), signal.SIGKILL)
    replace(source, destination)
    os.kill(os.getpid(), signal.SIGKILL)
exporting.os.replace = crash_replace
exporting.export_catalog(sys.argv[1], 'run', sys.argv[2])
"""


@pytest.mark.skipif(not hasattr(signal, "SIGKILL"), reason="Requires an uncatchable process exit")
@pytest.mark.parametrize("phase", ["before", "after"])
def test_forced_process_exit_exposes_only_complete_old_or_new_json_and_allows_reexport(source, tmp_path, phase):
    target = tmp_path / "exports" / "catalog.json"
    target.parent.mkdir()
    previous = b'{"previous": true}\n'
    target.write_bytes(previous)
    expected = read_catalog(source, "run")
    before = source.read_bytes()
    environment = dict(os.environ, PYTHONDONTWRITEBYTECODE="1",
                       PYTHONPATH=str(Path(__file__).resolve().parents[1] / "src"))
    crashed = subprocess.run([sys.executable, "-B", "-c", _CRASH_WORKER, str(source), str(target), phase],
                             env=environment, capture_output=True, text=True, timeout=15)
    assert crashed.returncode == -signal.SIGKILL, crashed.stderr
    if phase == "before":
        assert target.read_bytes() == previous
        temporary, = target.parent.glob(".*.tmp")
        assert json.loads(temporary.read_bytes()) == expected
    else:
        assert json.loads(target.read_bytes()) == expected
        assert list(target.parent.glob(".*.tmp")) == []
    export_catalog(source, "run", target)
    assert json.loads(target.read_bytes()) == expected
    assert source.read_bytes() == before
