"""Durable run state and atomic catalog commits for one local collector."""

from __future__ import annotations

import fcntl
import hashlib
import json
import logging
import sqlite3
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY NOT NULL CHECK (length(trim(run_id)) > 0),
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL CHECK (status IN
        ('running', 'interrupted', 'complete', 'partial', 'failed')),
    stop_reason TEXT,
    code_version TEXT NOT NULL CHECK (length(trim(code_version)) > 0),
    config_hash TEXT NOT NULL,
    config_json TEXT NOT NULL CHECK (json_type(config_json) = 'object'),
    budget_json TEXT NOT NULL CHECK (json_type(budget_json) = 'object')
);
CREATE TABLE IF NOT EXISTS tasks (
    task_id INTEGER PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    kind TEXT NOT NULL CHECK (kind IN ('discovery', 'product')),
    target_key TEXT NOT NULL CHECK (length(trim(target_key)) > 0),
    request_json TEXT NOT NULL CHECK (json_type(request_json) = 'object'),
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN
        ('pending', 'running', 'complete', 'partial', 'failed', 'deferred')),
    stage TEXT NOT NULL DEFAULT 'fetch' CHECK (stage IN ('fetch', 'extract', 'recovery')),
    discovered_from_json TEXT NOT NULL DEFAULT '[]'
        CHECK (json_type(discovered_from_json) = 'array'),
    checkpoint_json TEXT NOT NULL DEFAULT '{}' CHECK (json_type(checkpoint_json) = 'object'),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_retry_at TEXT,
    expected_item_count INTEGER CHECK (expected_item_count >= 0),
    decoded_item_count INTEGER CHECK (decoded_item_count >= 0),
    issues_json TEXT NOT NULL DEFAULT '[]' CHECK (json_type(issues_json) = 'array'),
    UNIQUE (run_id, kind, target_key)
);
CREATE TABLE IF NOT EXISTS task_events (
    event_id INTEGER PRIMARY KEY,
    task_id INTEGER NOT NULL REFERENCES tasks(task_id),
    attempt_no INTEGER NOT NULL CHECK (attempt_no > 0),
    type TEXT NOT NULL CHECK (type IN ('fetch', 'extract', 'recovery')),
    status TEXT NOT NULL CHECK (status IN ('started', 'succeeded', 'failed', 'uncertain')),
    started_at TEXT NOT NULL,
    finished_at TEXT,
    details_json TEXT NOT NULL CHECK (json_type(details_json) = 'object'),
    body BLOB,
    content_hash TEXT,
    UNIQUE (event_id, task_id),
    CHECK ((body IS NULL AND content_hash IS NULL) OR
           (type = 'fetch' AND body IS NOT NULL AND content_hash IS NOT NULL))
);
CREATE TABLE IF NOT EXISTS catalog_items (
    item_id INTEGER PRIMARY KEY,
    task_id INTEGER NOT NULL REFERENCES tasks(task_id),
    extraction_event_id INTEGER NOT NULL,
    product_name TEXT NOT NULL CHECK (length(trim(product_name)) > 0),
    product_url TEXT NOT NULL CHECK (length(trim(product_url)) > 0),
    captured_at TEXT NOT NULL,
    category_paths_json TEXT CHECK (category_paths_json IS NULL OR
        json_type(category_paths_json) = 'array'),
    sku TEXT NOT NULL CHECK (length(trim(sku)) > 0),
    item_name TEXT NOT NULL CHECK (length(trim(item_name)) > 0),
    item_description TEXT,
    manufacturer TEXT,
    manufacturer_part_number TEXT,
    selling_unit TEXT,
    pack_size TEXT,
    availability TEXT,
    currency TEXT,
    base_price TEXT,
    quantity_tiers_json TEXT NOT NULL CHECK (json_type(quantity_tiers_json) = 'array'),
    notes_json TEXT NOT NULL CHECK (json_type(notes_json) = 'array'),
    UNIQUE (task_id, sku),
    FOREIGN KEY (extraction_event_id, task_id) REFERENCES task_events(event_id, task_id)
);
CREATE INDEX IF NOT EXISTS tasks_pending ON tasks(run_id, status, next_retry_at);
CREATE INDEX IF NOT EXISTS events_by_task ON task_events(task_id, event_id);
"""

ITEM_FIELDS = (
    "product_name", "product_url", "captured_at", "category_paths_json", "sku",
    "item_name", "item_description", "manufacturer", "manufacturer_part_number",
    "selling_unit", "pack_size", "availability", "currency", "base_price",
    "quantity_tiers_json", "notes_json",
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False)


def _hash(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _decoded(row: sqlite3.Row) -> dict[str, Any]:
    return {
        key.removesuffix("_json"): json.loads(row[key]) if row[key] is not None else None
        for key in row.keys() if key.endswith("_json")
    } | {key: row[key] for key in row.keys() if not key.endswith("_json")}


@dataclass(frozen=True)
class TaskSpec:
    """Task input; callers supply a stable target key and non-secret request data."""

    kind: str
    target_key: str
    request: Mapping[str, Any]
    discovered_from: Sequence[Mapping[str, Any]] = ()


class Storage:
    """Own one database exclusively on macOS/Linux; use it from one thread.

    SQLite errors propagate to the controller. No method performs network calls.
    JSON fields returned by read methods are decoded and lose their _json suffix.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # Ownership must not interfere with SQLite's own locks on the database.
        self._owner = self.path.with_name(self.path.name + ".lock").open("a+b")
        try:
            fcntl.flock(self._owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            self._owner.close()
            raise RuntimeError(f"Another collector owns {self.path}") from error
        try:
            self._db = sqlite3.connect(self.path, isolation_level=None, timeout=5.0)
            self._db.row_factory = sqlite3.Row
            existing = {row["name"] for row in self._db.execute("PRAGMA table_info(catalog_items)")}
            expected = {"item_id", "task_id", "extraction_event_id", *ITEM_FIELDS}
            if existing and existing != expected:
                raise ValueError("Incompatible catalog schema; preserve this run and use a new database")
            self._db.execute("PRAGMA foreign_keys = ON")
            self._db.execute("PRAGMA journal_mode = DELETE")
            self._db.execute("PRAGMA synchronous = EXTRA")
            self._db.executescript("BEGIN IMMEDIATE;\n" + SCHEMA + "\nCOMMIT;")
        except BaseException:
            if hasattr(self, "_db"):
                self._db.close()
            self._owner.close()
            raise
        logger.info("Opened checkpoint database path=%s", self.path)

    def __enter__(self) -> Storage:
        """Return the exclusively owned store."""
        return self

    def __exit__(self, *exc: Any) -> None:
        """Close the database and release ownership without suppressing errors."""
        self.close()

    def close(self) -> None:
        """Release the connection and ownership; safe to call more than once."""
        try:
            self._db.close()
        finally:
            self._owner.close()

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        self._db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self._db.execute("COMMIT")
        except BaseException:
            if self._db.in_transaction:
                self._db.execute("ROLLBACK")
            logger.debug("Rolled back storage transaction")
            raise

    def _row(self, table: str, column: str, key: Any) -> sqlite3.Row:
        row = self._db.execute(f"SELECT * FROM {table} WHERE {column} = ?", (key,)).fetchone()
        if row is None:
            raise KeyError(f"Unknown {table}.{column}: {key}")
        return row

    def create_run(self, run_id: str, config: Mapping[str, Any], code_version: str,
                   *, budget: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """Create the database's only run; existing runs must be resumed explicitly."""
        config_text = _json(dict(config))
        with self._transaction():
            if self._db.execute("SELECT 1 FROM runs").fetchone():
                raise ValueError("Database already contains a run; use resume_run")
            self._db.execute(
                "INSERT INTO runs (run_id, started_at, status, code_version, config_hash, "
                "config_json, budget_json) VALUES (?, ?, 'running', ?, ?, ?, ?)",
                (run_id, _now(), code_version, _hash(config_text.encode()), config_text,
                 _json(dict(budget or {}))),
            )
        logger.info("Created run run_id=%s", run_id)
        return self.get_run(run_id)

    def get_run(self, run_id: str) -> dict[str, Any]:
        """Return saved run settings, status, and budget or raise KeyError."""
        return _decoded(self._row("runs", "run_id", run_id))

    def resume_run(self, run_id: str, config: Mapping[str, Any],
                   code_version: str) -> dict[str, Any]:
        """Resume an unfinished compatible run; preserve terminal tasks and budgets.

        Started operations become uncertain. Running tasks return to pending with
        their saved stage/evidence intact; the controller reconciles uncertain calls.
        """
        with self._transaction():
            run = self.get_run(run_id)
            if run["code_version"] != code_version or run["config_hash"] != _hash(
                _json(dict(config)).encode()
            ):
                raise ValueError("Resume requires the original code and configuration")
            if run["status"] not in {"running", "interrupted"}:
                raise ValueError("Run is terminal; explicit retry policy is required")
            uncertain = self._db.execute(
                "UPDATE task_events SET status = 'uncertain', finished_at = ? "
                "WHERE status = 'started' AND task_id IN "
                "(SELECT task_id FROM tasks WHERE run_id = ?)", (_now(), run_id),
            ).rowcount
            pending = self._db.execute(
                "UPDATE tasks SET status = 'pending' WHERE run_id = ? AND status = 'running'",
                (run_id,),
            ).rowcount
            self._db.execute(
                "UPDATE runs SET status = 'running', finished_at = NULL, stop_reason = NULL "
                "WHERE run_id = ?", (run_id,),
            )
        logger.info("Resumed run run_id=%s requeued=%d uncertain_operations=%d",
                    run_id, pending, uncertain)
        return self.get_run(run_id)

    def enable_recovery(self, run_id: str, config: Mapping[str, Any], code_version: str) -> int:
        """Explicitly continue a stopped deterministic run with bounded recovery.

        Permit only a partial/failed run whose recovery was deferred and whose
        model budget is unused. Collection settings stay unchanged. Requeue
        unresolved product tasks with intact saved fetches; retain accepted rows,
        source checkpoints, attempts and budgets. Pending collection stays pending.
        Record the old policy/code identity with each requeued task and adopt the
        current code for re-extraction. Commit settings, audit and task changes
        together; return the requeued count. A crash afterward uses normal resume.
        Do not enable twice, reset a consumed budget or retry an uncertain call.
        """
        settings = dict(config)
        with self._transaction():
            run = self.get_run(run_id)
            if run["status"] not in {"partial", "failed"} or run["config"].get("recovery", {}).get("mode") != "deferred":
                raise ValueError("Recovery can only be enabled once on a stopped deterministic run; use resume after interruption")
            if any(run["budget"].values()):
                raise ValueError("Enabling recovery cannot reset an existing budget")
            if settings.get("recovery", {}).get("mode") != "hosted" or not settings.get("recovery_endpoint"):
                raise ValueError("Supply the recovery endpoint and hosted policy")
            policy_keys = {"recovery", "recovery_endpoint"}
            old_collection = {key: value for key, value in run["config"].items() if key not in policy_keys}
            new_collection = {key: value for key, value in settings.items() if key not in policy_keys}
            if new_collection != old_collection:
                raise ValueError("Enabling recovery cannot change collection settings")
            eligible = []
            for task in self.list_tasks(run_id):
                source_id = task["checkpoint"].get("source_event_id")
                if task["kind"] != "product" or task["status"] not in {"partial", "failed", "deferred"} or source_id is None:
                    continue
                source = self.get_event(source_id)
                if (source["task_id"] != task["task_id"] or source["type"] != "fetch" or source["status"] != "succeeded"
                        or source["body"] is None or source["details"].get("body_complete") is not True
                        or not 200 <= source["details"].get("http_status", 0) < 300
                        or _hash(source["body"]) != source["content_hash"]):
                    raise ValueError("Recovery requires intact successful source checkpoints")
                eligible.append(task)
            if not eligible:
                raise ValueError("No unresolved products have saved source evidence for recovery")
            config_text, timestamp = _json(settings), _now()
            self._db.execute(
                "UPDATE runs SET config_json = ?, config_hash = ?, code_version = ?, "
                "status = 'running', finished_at = NULL, stop_reason = NULL WHERE run_id = ?",
                (config_text, _hash(config_text.encode()), code_version, run_id))
            for task in eligible:
                audit = {"operation": "enable_recovery", "previous_task_status": task["status"],
                         "previous_run_status": run["status"], "previous_stop_reason": run["stop_reason"],
                         "previous_config_hash": run["config_hash"], "previous_code_version": run["code_version"],
                         "previous_recovery": run["config"]["recovery"],
                         "previous_recovery_endpoint": run["config"].get("recovery_endpoint")}
                self._db.execute(
                    "INSERT INTO task_events (task_id, attempt_no, type, status, started_at, finished_at, details_json) "
                    "VALUES (?, ?, 'recovery', 'succeeded', ?, ?, ?)",
                    (task["task_id"], task["attempt_count"], timestamp, timestamp, _json(audit)))
                self._db.execute(
                    "UPDATE tasks SET status = 'pending', stage = 'extract', next_retry_at = NULL WHERE task_id = ?",
                    (task["task_id"],))
        logger.info("Recovery enabled run_id=%s requeued_products=%d accepted_rows_preserved=true",
                    run_id, len(eligible))
        return len(eligible)

    def finish_run(self, run_id: str, status: str, *, reason: str | None = None) -> None:
        """Record complete/partial/failed/interrupted; the controller determines coverage."""
        if status not in {"complete", "partial", "failed", "interrupted"}:
            raise ValueError("Invalid run outcome")
        with self._transaction():
            self._row("runs", "run_id", run_id)
            self._db.execute(
                "UPDATE runs SET status = ?, finished_at = ?, stop_reason = ? WHERE run_id = ?",
                (status, None if status == "interrupted" else _now(), reason, run_id),
            )
        logger.info("Run outcome run_id=%s status=%s reason=%s", run_id, status, reason)

    def _enqueue(self, run_id: str, spec: TaskSpec) -> int:
        request = _json(dict(spec.request))
        self._db.execute(
            "INSERT INTO tasks (run_id, kind, target_key, request_json) VALUES (?, ?, ?, ?) "
            "ON CONFLICT(run_id, kind, target_key) DO NOTHING",
            (run_id, spec.kind, spec.target_key, request),
        )
        row = self._db.execute(
            "SELECT * FROM tasks WHERE run_id = ? AND kind = ? AND target_key = ?",
            (run_id, spec.kind, spec.target_key),
        ).fetchone()
        if row["request_json"] != request:
            raise ValueError("A task target cannot be reused with different request parameters")
        origins = json.loads(row["discovered_from_json"])
        for origin in spec.discovered_from:
            if dict(origin) not in origins:
                origins.append(dict(origin))
        self._db.execute("UPDATE tasks SET discovered_from_json = ? WHERE task_id = ?",
                         (_json(origins), row["task_id"]))
        return row["task_id"]

    def enqueue_task(self, run_id: str, spec: TaskSpec) -> int:
        """Insert or reuse one task, merging origins without resetting its progress."""
        with self._transaction():
            task_id = self._enqueue(run_id, spec)
        logger.debug("Queued task run_id=%s task_id=%d kind=%s", run_id, task_id, spec.kind)
        return task_id

    def get_task(self, task_id: int) -> dict[str, Any]:
        """Return one task and decoded checkpoint fields or raise KeyError."""
        return _decoded(self._row("tasks", "task_id", task_id))

    def list_tasks(self, run_id: str) -> list[dict[str, Any]]:
        """Return the run's worklist in stable creation order."""
        return [_decoded(row) for row in self._db.execute(
            "SELECT * FROM tasks WHERE run_id = ? ORDER BY task_id", (run_id,))]

    def set_task_state(self, task_id: int, status: str, *, stage: str | None = None,
                       issues: Sequence[Any] | None = None,
                       next_retry_at: str | None = None) -> None:
        """Record failure, deferral, or an explicit retry decision, never completion.

        The caller enforces retry/budget policy. Use commit_product or
        commit_discovery to mark accepted work complete atomically.
        """
        if status not in {"pending", "partial", "failed", "deferred"}:
            raise ValueError("Invalid task state")
        with self._transaction():
            task = self.get_task(task_id)
            has_items = self._db.execute(
                "SELECT 1 FROM catalog_items WHERE task_id = ?", (task_id,)
            ).fetchone() is not None
            if status == "partial" and not has_items:
                raise ValueError("Partial tasks require accepted catalog records")
            if status == "failed" and has_items:
                raise ValueError("Tasks with accepted records must be partial, not failed")
            self._db.execute(
                "UPDATE tasks SET status = ?, stage = ?, issues_json = ?, next_retry_at = ? "
                "WHERE task_id = ?",
                (status, stage or task["stage"],
                 _json(list(issues) if issues is not None else task["issues"]),
                 next_retry_at, task_id),
            )
        logger.warning("Task state task_id=%d status=%s stage=%s", task_id, status,
                       stage or task["stage"])

    def start_event(self, task_id: int, event_type: str, *,
                    details: Mapping[str, Any] | None = None,
                    budget: Mapping[str, Any] | None = None) -> int:
        """Persist intent before execution; optionally commit a reserved run budget.

        Starting pending work increments its attempt once. Subsequent operations
        on that running task share the attempt. Terminal tasks require explicit retry.
        """
        with self._transaction():
            task = self.get_task(task_id)
            if task["status"] not in {"pending", "running"}:
                raise ValueError("Task must be pending or running")
            attempt = task["attempt_count"] + (task["status"] == "pending")
            self._db.execute(
                "UPDATE tasks SET status = 'running', stage = ?, attempt_count = ?, "
                "next_retry_at = NULL WHERE task_id = ?", (event_type, attempt, task_id),
            )
            event_id = self._db.execute(
                "INSERT INTO task_events (task_id, attempt_no, type, status, started_at, "
                "details_json) VALUES (?, ?, ?, 'started', ?, ?)",
                (task_id, attempt, event_type, _now(), _json(dict(details or {}))),
            ).lastrowid
            self._save_budget(task["run_id"], budget)
        logger.debug("Started operation task_id=%d event_id=%d type=%s attempt=%d",
                     task_id, event_id, event_type, attempt)
        return event_id

    def _save_budget(self, run_id: str, budget: Mapping[str, Any] | None) -> None:
        if budget is not None:
            self._db.execute("UPDATE runs SET budget_json = ? WHERE run_id = ?",
                             (_json(dict(budget)), run_id))

    def update_event(self, event_id: int, status: str, *,
                     details: Mapping[str, Any] | None = None, body: bytes | None = None,
                     checkpoint: Mapping[str, Any] | None = None,
                     budget: Mapping[str, Any] | None = None) -> None:
        """Save observations/evidence and checkpoint/budget changes together.

        Details and checkpoints merge at the top level. Source promotion is an
        explicit caller checkpoint; saving additional evidence never changes it.
        Use status started for intermediate recovery steps; use commit methods
        for accepted extraction.
        """
        if status not in {"started", "succeeded", "failed", "uncertain"}:
            raise ValueError("Invalid operation status")
        with self._transaction():
            event = _decoded(self._row("task_events", "event_id", event_id))
            task = self.get_task(event["task_id"])
            if event["status"] not in {"started", "uncertain"}:
                raise ValueError("Operation already finished")
            if event["attempt_no"] != task["attempt_count"]:
                raise ValueError("Operation belongs to an older task attempt")
            if event["type"] == "extract" and status == "succeeded":
                raise ValueError("Accept extraction with a commit method")
            saved_body = event["body"] if body is None else body
            if saved_body is not None and event["type"] != "fetch":
                raise ValueError("Only fetch operations store response bodies")
            if event["type"] == "fetch" and status == "succeeded" and saved_body is None:
                raise ValueError("Successful fetch requires saved response bytes")
            self._db.execute(
                "UPDATE task_events SET status = ?, finished_at = ?, details_json = ?, "
                "body = ?, content_hash = ? WHERE event_id = ?",
                (status, None if status == "started" else _now(),
                 _json(event["details"] | dict(details or {})), saved_body,
                 None if saved_body is None else _hash(saved_body), event_id),
            )
            saved_checkpoint = task["checkpoint"] | dict(checkpoint or {})
            self._db.execute("UPDATE tasks SET checkpoint_json = ? WHERE task_id = ?",
                             (_json(saved_checkpoint), task["task_id"]))
            self._save_budget(task["run_id"], budget)
        log = logger.warning if status in {"failed", "uncertain"} else logger.debug
        log("Operation state task_id=%d event_id=%d type=%s status=%s",
            task["task_id"], event_id, event["type"], status)

    def _acceptance(self, event_id: int, kind: str,
                    fingerprint: str) -> tuple[dict[str, Any], bool]:
        event = _decoded(self._row("task_events", "event_id", event_id))
        task = self.get_task(event["task_id"])
        if event["type"] != "extract" or task["kind"] != kind:
            raise ValueError("Acceptance requires the matching extraction operation")
        if (event["status"] == "succeeded" or
                event["status"] == "failed" and event["details"].get("result_hash") is not None):
            if event["details"].get("result_hash") != fingerprint:
                raise ValueError("Committed operation cannot accept a different result")
            return task, True
        if (event["status"] != "started" or task["status"] != "running"
                or event["attempt_no"] != task["attempt_count"]):
            raise ValueError("Acceptance requires the current running attempt")
        return task, False

    def _finish_extraction(self, event_id: int, details: Mapping[str, Any], *,
                           status: str = "succeeded") -> None:
        event = _decoded(self._row("task_events", "event_id", event_id))
        self._db.execute(
            "UPDATE task_events SET status = ?, finished_at = ?, details_json = ? "
            "WHERE event_id = ?", (status, _now(), _json(event["details"] | dict(details)), event_id),
        )

    def commit_discovery(self, event_id: int, targets: Sequence[TaskSpec], *,
                         checkpoint: Mapping[str, Any] | None = None,
                         details: Mapping[str, Any] | None = None) -> list[int]:
        """Commit discovered products/next-page tasks and page completion together.

        Targets may include the next discovery request. Repeating the same accepted
        operation is a no-op; changing its payload is rejected.
        """
        payload = {"targets": [dict(kind=s.kind, target_key=s.target_key,
                                    request=dict(s.request), discovered_from=list(s.discovered_from))
                                for s in targets], "checkpoint": dict(checkpoint or {}),
                   "details": dict(details or {})}
        fingerprint = _hash(_json(payload).encode())
        with self._transaction():
            task, repeated = self._acceptance(event_id, "discovery", fingerprint)
            if repeated:
                event = _decoded(self._row("task_events", "event_id", event_id))
                return event["details"]["target_ids"]
            target_ids = list(dict.fromkeys(self._enqueue(task["run_id"], s) for s in targets))
            self._finish_extraction(event_id, dict(details or {}) |
                                    {"result_hash": fingerprint, "target_ids": target_ids})
            self._db.execute(
                "UPDATE tasks SET status = 'complete', checkpoint_json = ?, "
                "issues_json = '[]', next_retry_at = NULL WHERE task_id = ?",
                (_json(task["checkpoint"] | dict(checkpoint or {})), task["task_id"]),
            )
        logger.info("Discovery checkpoint run_id=%s task_id=%d targets=%d",
                    task["run_id"], task["task_id"], len(target_ids))
        return target_ids

    def commit_product(self, event_id: int, items: Sequence[Mapping[str, Any]], *,
                       status: str = "complete", expected_item_count: int | None = None,
                       decoded_item_count: int | None = None,
                       issues: Sequence[Any] = ()) -> int:
        """Atomically replace validated flat SKU rows and their task outcome.

        Input keys follow catalog_items without internal IDs; JSON fields use
        category_paths, quantity_tiers and notes. Unit/pack fields are nullable
        text with no unit allowlist. Omitted notes default to an empty array.
        The caller finalizes notes after validation and any eligible recovery;
        recovery references/deferral reasons belong in the extraction event details.
        Required identity/JSON constraints and duplicate SKUs are enforced here;
        note contents and business validation belong to the caller.
        """
        if status not in {"complete", "partial"} or not items:
            raise ValueError("Accepted products require items and a complete/partial outcome")
        payload = {"items": list(items), "status": status, "expected": expected_item_count,
                   "decoded": decoded_item_count, "issues": list(issues)}
        fingerprint = _hash(_json(payload).encode())
        with self._transaction():
            task, repeated = self._acceptance(event_id, "product", fingerprint)
            if repeated:
                return len(items)
            self._db.execute("DELETE FROM catalog_items WHERE task_id = ?", (task["task_id"],))
            columns = ("task_id", "extraction_event_id") + ITEM_FIELDS
            sql = (f"INSERT INTO catalog_items ({', '.join(columns)}) VALUES "
                   f"({', '.join('?' for _ in columns)})")
            note_count = 0
            for item in items:
                row = dict(item)
                paths = row.pop("category_paths", None)
                row["category_paths_json"] = None if paths is None else _json(paths)
                row["quantity_tiers_json"] = _json(row.pop("quantity_tiers", []))
                notes = row.pop("notes", [])
                row["notes_json"] = _json(notes)
                self._db.execute(sql, (task["task_id"], event_id) +
                                 tuple(row.get(field) for field in ITEM_FIELDS))
                note_count += len(notes)
            self._finish_extraction(event_id, {"result_hash": fingerprint,
                                    "accepted_count": len(items), "issues": list(issues)})
            self._db.execute(
                "UPDATE tasks SET status = ?, expected_item_count = ?, decoded_item_count = ?, "
                "issues_json = ?, next_retry_at = NULL WHERE task_id = ?",
                (status, expected_item_count, decoded_item_count, _json(list(issues)), task["task_id"]),
            )
        logger.info("Product checkpoint run_id=%s task_id=%d accepted=%d expected=%s "
                    "status=%s notes=%d", task["run_id"], task["task_id"], len(items),
                    expected_item_count, status, note_count)
        return len(items)

    def fail_product(self, event_id: int, *, expected_item_count: int | None = None,
                     decoded_item_count: int | None = None, issues: Sequence[Any] = (),
                     details: Mapping[str, Any] | None = None,
                     status: str = "failed", stage: str | None = None) -> str:
        """Finalize failed extraction, its known counts and task outcome atomically.

        None means unknown; zero means a known empty count. Keep previously
        accepted rows and mark their task partial; otherwise use failed/deferred
        as requested by the controller. An optional stage updates its checkpoint.
        Return that task status. Repeating an identical finalization is a no-op;
        changing its result is rejected. Counts must be nonnegative integers or
        None. No source wording, diagnostic text or business rules are inferred.
        """
        if status not in {"failed", "deferred"}:
            raise ValueError("Failed extraction requires a failed/deferred task outcome")
        for count in (expected_item_count, decoded_item_count):
            if count is not None and (type(count) is not int or count < 0):
                raise ValueError("Item counts must be nonnegative integers or None")
        payload = {"failed": True, "status": status, "stage": stage, "expected": expected_item_count,
                   "decoded": decoded_item_count, "issues": list(issues),
                   "details": dict(details or {})}
        fingerprint = _hash(_json(payload).encode())
        with self._transaction():
            task, repeated = self._acceptance(event_id, "product", fingerprint)
            if repeated:
                return task["status"]
            retained = self._db.execute(
                "SELECT count(*) FROM catalog_items WHERE task_id = ?", (task["task_id"],)
            ).fetchone()[0]
            status = "partial" if retained else status
            self._finish_extraction(event_id, dict(details or {}) | {
                "result_hash": fingerprint, "expected_item_count": expected_item_count,
                "decoded_item_count": decoded_item_count, "accepted_count": 0,
                "retained_item_count": retained, "issues": list(issues),
            }, status="failed")
            self._db.execute(
                "UPDATE tasks SET status = ?, stage = ?, expected_item_count = ?, decoded_item_count = ?, "
                "issues_json = ?, next_retry_at = NULL WHERE task_id = ?",
                (status, stage or task["stage"], expected_item_count, decoded_item_count,
                 _json(list(issues)), task["task_id"]),
            )
        logger.warning("Product checkpoint run_id=%s task_id=%d status=%s expected=%s "
                       "decoded=%s retained=%d", task["run_id"], task["task_id"], status,
                       expected_item_count, decoded_item_count, retained)
        return status

    def list_items(self, task_id: int) -> list[dict[str, Any]]:
        """Return accepted rows ordered by SKU; internal IDs remain for evidence lookup."""
        return [_decoded(row) for row in self._db.execute(
            "SELECT * FROM catalog_items WHERE task_id = ? ORDER BY sku", (task_id,))]

    def get_event(self, event_id: int) -> dict[str, Any]:
        """Return one saved operation, including its body, or raise KeyError."""
        return _decoded(self._row("task_events", "event_id", event_id))

    def list_events(self, task_id: int, *, include_bodies: bool = True) -> list[dict[str, Any]]:
        """Return ordered operations with decoded details.

        Set include_bodies=False for scheduling and reports. That query omits the
        body field without reading its bytes; content_hash remains available.
        Use get_event to load one selected operation's evidence.
        """
        columns = "*" if include_bodies else (
            "event_id, task_id, attempt_no, type, status, started_at, finished_at, "
            "details_json, content_hash")
        return [_decoded(row) for row in self._db.execute(
            f"SELECT {columns} FROM task_events WHERE task_id = ? ORDER BY event_id", (task_id,))]
