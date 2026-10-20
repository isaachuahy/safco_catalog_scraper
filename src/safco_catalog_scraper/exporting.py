"""Read committed catalog observations and atomically publish a JSON snapshot."""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import tempfile
from contextlib import closing
from pathlib import Path
from typing import Any

from .storage import ITEM_FIELDS

logger = logging.getLogger(__name__)


def read_catalog(database_path: str | Path, run_id: str) -> dict[str, Any]:
    """Return a consistent, committed snapshot as {run, products} without writes.

    Each product contains product_url, product_name and items sorted by string
    SKU. Items retain every other catalog field, including per-observation
    captured_at/category_paths, nullable values, decimal-string prices, tiers
    and notes. Internal database IDs and raw execution evidence are excluded.
    Equal SKUs on different product URLs remain separate observations.

    Run metadata retains the saved status, times, code/configuration versions,
    non-secret configuration and recovery budget. Running/interrupted runs may
    be exported; this does not finalize or resume them. Products without accepted
    rows are absent; unresolved work remains in the separate run report/database.
    No export timestamp, calculated catalog fields or schema_version are added.

    Raise KeyError for an unknown run, ValueError for an invalid run ID or
    conflicting family names on one URL, and sqlite3.Error for database/schema
    failures. A missing database is never created. No network/model calls occur.
    """
    if not isinstance(run_id, str) or not run_id.strip() or run_id != run_id.strip():
        raise ValueError("Supply a nonempty run ID without surrounding whitespace")
    source = Path(database_path).resolve()
    with closing(sqlite3.connect(source.as_uri() + "?mode=ro", uri=True, isolation_level=None)) as database:
        database.row_factory = sqlite3.Row
        # Read metadata and rows from the same commit even while collection continues.
        database.execute("BEGIN")
        saved_run = database.execute(
            "SELECT run_id, started_at, finished_at, status, stop_reason, code_version, "
            "config_hash, config_json, budget_json FROM runs WHERE run_id = ?", (run_id,),
        ).fetchone()
        if saved_run is None:
            raise KeyError("Unknown run: " + run_id)
        run = dict(saved_run)
        for name in ("config", "budget"):
            run[name] = json.loads(run.pop(name + "_json"))
        columns = ", ".join("item." + field for field in ITEM_FIELDS)
        rows = database.execute(
            "SELECT " + columns + " FROM catalog_items AS item JOIN tasks AS task "
            "ON item.task_id = task.task_id WHERE task.run_id = ? AND task.kind = 'product' "
            "ORDER BY item.product_url, item.sku", (run_id,),
        )
        products: dict[str, dict[str, Any]] = {}
        for row in rows:
            item = {name.removesuffix("_json"): json.loads(row[name])
                    if name.endswith("_json") and row[name] is not None else row[name]
                    for name in ITEM_FIELDS}
            url, name = item.pop("product_url"), item.pop("product_name")
            if url not in products:
                products[url] = {"product_url": url, "product_name": name, "items": []}
            elif products[url]["product_name"] != name:
                raise ValueError("Conflicting product family names for " + url)
            products[url]["items"].append(item)
    return {"run": run, "products": list(products.values())}


def export_catalog(database_path: str | Path, run_id: str, output_path: str | Path) -> Path:
    """Write read_catalog's UTF-8 JSON with stable ordering; return its absolute path.

    The caller chooses the dated output path. Exporting the same committed state
    repeatedly produces identical bytes. Existing output is replaced only after
    a temporary file in its directory has been serialized, flushed and synced.
    Sync the parent directory after replacement. Failures propagate; before
    replacement they leave an existing output intact. A directory-sync failure
    can occur after replacement, leaving the new file visible but durability
    unconfirmed. Clean temporary files on catchable failures; SIGKILL can leave
    a harmless temporary file. Parent directories are created when needed.

    Reject database/sidecar destinations, including symlinks and hard links.
    This does not alter source data, task state, configuration or budgets, and
    does not generate the separate run report. No CLI wiring occurs here.
    """
    source, output = Path(database_path).resolve(), Path(output_path).resolve()
    protected = [source.with_name(source.name + suffix)
                 for suffix in ("", ".lock", "-journal", "-wal", "-shm")]
    for path in protected:
        if output == path or (output.exists() and path.exists() and output.samefile(path)):
            raise ValueError("Catalog output cannot replace the database or its sidecar files")
    catalog = read_catalog(source, run_id)
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", newline="\n", dir=output.parent,
                                         prefix="." + output.name + ".", suffix=".tmp", delete=False) as stream:
            temporary = Path(stream.name)
            json.dump(catalog, stream, ensure_ascii=False, allow_nan=False, sort_keys=True, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, output)
        directory = os.open(output.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    logger.info("Catalog exported run_id=%s status=%s products=%d items=%d output=%s",
                run_id, catalog["run"]["status"], len(catalog["products"]),
                sum(len(product["items"]) for product in catalog["products"]), output)
    return output
