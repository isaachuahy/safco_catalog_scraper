"""Run with python -m safco_catalog_scraper; logs use stderr, reports use stdout."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
from collections.abc import Sequence
from pathlib import Path

from .exporting import export_catalog
from .diagnostics import failure_diagnostic, failure_location
from .recovery import PROMPT_VERSION, SERVICE_VERSION, RecoveryPolicy
from .runner import DEFAULT_CATEGORIES, RunConfig, RunSetupError, run_catalog
from .storage import Storage

logger = logging.getLogger("safco_catalog_scraper")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m safco_catalog_scraper",
        description="Collect Safco products or export a saved SQLite run.",
        epilog="Logs go to stderr. Run/resume/recover print a JSON run report to stdout; "
               "export prints a JSON receipt. Catalog JSON is written to --output. "
               "Exit codes: 0 complete collection or successful export, "
               "1 partial/failed collection or execution/export error, "
               "2 invalid invocation/configuration, 130 interrupted. "
               "Export preserves the saved run status, including partial/interrupted runs. "
               "Optional hosted recovery uses saved evidence and bounded allowances.",
    )
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--database", type=Path, required=True,
                        help="SQLite path for this run; use a separate database for each new run")
    common.add_argument("--run-id", required=True, help="Run identity retained across restarts")
    common.add_argument("--log-level", choices=("DEBUG", "INFO", "WARNING", "ERROR"), default="INFO",
                        help="Console log level (default: INFO)")
    commands = parser.add_subparsers(dest="command", required=True)
    new = commands.add_parser("run", parents=[common], help="Start a new run in an unused database")
    resume = commands.add_parser("resume", parents=[common],
                                 help="Resume an interrupted run using its saved settings and original code")
    recover = commands.add_parser("recover", parents=[common],
                                  help="Enable bounded recovery on a stopped deterministic run; reuse saved evidence")
    export = commands.add_parser("export", parents=[common],
                                 help="Export committed catalog rows without collecting or resuming work")
    for command in (new, resume, recover, export):
        command.add_argument("--output", type=Path, required=command is export, metavar="PATH",
                             help="Catalog JSON destination; use a dated filename; replaced atomically")
    new.add_argument("--unresolved-product-limit", type=int,
                     help="Optional stop after this many partial/failed/deferred products; omit to attempt all")
    new.add_argument("--category", dest="categories", action="append", metavar="URL",
                     help="Safco category URL; repeat to override the two default categories")
    new.add_argument("--max-fetch-attempts", type=int, default=RunConfig.max_fetch_attempts,
                     help="Attempts per request, including uncertain requests (default: %(default)s)")
    new.add_argument("--request-interval-seconds", type=float, default=RunConfig.request_interval_seconds,
                     help="Minimum spacing between requests (default: %(default)s)")
    new.add_argument("--retry-base-seconds", type=float, default=RunConfig.retry_base_seconds,
                     help="Initial exponential retry delay (default: %(default)s)")
    new.add_argument("--retry-max-seconds", type=float, default=RunConfig.retry_max_seconds,
                     help="Backoff cap; later Retry-After deadlines still apply (default: %(default)s)")
    new.add_argument("--timeout-seconds", type=float, default=RunConfig.timeout_seconds,
                     help="HTTP timeout per phase (default: %(default)s)")
    new.add_argument("--max-response-bytes", type=int, default=RunConfig.max_response_bytes,
                     help="Maximum decoded response size (default: %(default)s)")
    for command in (new, recover):
        command.add_argument("--recovery-endpoint", default=os.environ.get("SAFCO_RECOVERY_ENDPOINT"),
                             help="Full HTTPS decision endpoint; defaults to SAFCO_RECOVERY_ENDPOINT")
        command.add_argument("--recovery-model", default=os.environ.get("SAFCO_RECOVERY_MODEL"),
                             help="Model identity configured at the endpoint; defaults to SAFCO_RECOVERY_MODEL")
        command.add_argument("--recovery-case-limit", type=int, default=3)
        command.add_argument("--recovery-turn-limit", type=int, default=12)
        command.add_argument("--recovery-case-turn-limit", type=int, default=4)
    return parser


def _config(arguments: argparse.Namespace) -> RunConfig:
    if arguments.command in {"resume", "recover"}:
        if not arguments.database.is_file():
            raise ValueError("Saved-run commands require an existing database: " + str(arguments.database))
        with Storage(arguments.database) as store:
            settings = store.get_run(arguments.run_id)["config"]
        recovery = settings.pop("recovery")
        if arguments.command == "recover":
            if not arguments.recovery_endpoint or not arguments.recovery_model:
                raise ValueError("Supply the recovery endpoint and its model identity to enable recovery")
            settings["recovery_endpoint"] = arguments.recovery_endpoint
            settings["recovery_policy"] = RecoveryPolicy(
                SERVICE_VERSION, arguments.recovery_model, PROMPT_VERSION,
                max_cases=arguments.recovery_case_limit, max_model_turns_per_run=arguments.recovery_turn_limit,
                max_model_turns_per_case=arguments.recovery_case_turn_limit)
        elif recovery["mode"] == "hosted":
            recovery.pop("mode")
            if recovery.pop("protocol_version") != 1:
                raise ValueError("Unsupported saved recovery protocol")
            settings["recovery_policy"] = RecoveryPolicy(**recovery)
        return RunConfig(**settings)
    if bool(arguments.recovery_endpoint) != bool(arguments.recovery_model):
        raise ValueError("Supply both a recovery endpoint and its configured model identity")
    return RunConfig(
        unresolved_product_limit=arguments.unresolved_product_limit,
        categories=tuple(arguments.categories) if arguments.categories else DEFAULT_CATEGORIES,
        max_fetch_attempts=arguments.max_fetch_attempts,
        request_interval_seconds=arguments.request_interval_seconds,
        retry_base_seconds=arguments.retry_base_seconds,
        retry_max_seconds=arguments.retry_max_seconds,
        timeout_seconds=arguments.timeout_seconds,
        max_response_bytes=arguments.max_response_bytes,
        recovery_endpoint=arguments.recovery_endpoint,
        recovery_policy=(RecoveryPolicy(SERVICE_VERSION, arguments.recovery_model, PROMPT_VERSION,
            max_cases=arguments.recovery_case_limit, max_model_turns_per_run=arguments.recovery_turn_limit,
            max_model_turns_per_case=arguments.recovery_case_turn_limit) if arguments.recovery_endpoint else None),
    )


def _export(arguments: argparse.Namespace) -> dict[str, str]:
    try:
        path = export_catalog(arguments.database, arguments.run_id, arguments.output)
    except Exception as error:
        known = isinstance(error, (OSError, sqlite3.Error, KeyError, TypeError, ValueError))
        reason = str(error) if known else "Unexpected export failure"
        logger.error("Catalog export failed run_id=%s output=%s error_type=%s reason=%s location=%s; "
                     "retry export after correcting the error", arguments.run_id, arguments.output,
                     type(error).__name__, reason, failure_location(error))
        return {"status": "failed", "output_path": str(arguments.output), "error_type": type(error).__name__}
    return {"status": "succeeded", "output_path": str(path)}


def main(argv: Sequence[str] | None = None) -> int:
    """Collect, resume, enable recovery or export without exiting the caller.

    Emit logs to stderr and a JSON run report or export receipt to stdout. Optional
    --output for run/resume/recover publishes a catalog after collection returns, including
    partial/failed outcomes. Add catalog_export with its status/path/error type to
    the report; export failures preserve the collection report and saved run state.
    Export alone reads committed data without resuming tasks or checking collector
    code compatibility, and requires --output. It emits {run_id, catalog_export}.

    Return 0 for complete collection or successful standalone export, 1 for
    partial/failed collection or execution/export errors, 2 for invalid collection
    settings, and 130 for a keyboard interruption. A successful export can contain
    a saved partial/interrupted run. Argument parsing raises SystemExit for help
    or syntax errors. Resume retains saved policy; terminal runs stay rejected.
    Recover explicitly enables recovery once on a stopped deterministic run,
    audits the policy change and reuses its saved evidence and accepted rows.
    An interruption during collection leaves progress in SQLite; request export
    separately to save it. Retrying export never requires collection to restart.
    """
    arguments = _parser().parse_args(argv)
    logging.basicConfig(level=arguments.log_level, stream=sys.stderr,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
                        datefmt="%Y-%m-%dT%H:%M:%S%z")
    # Fetcher already logs safe request metadata; suppress transport diagnostics even at DEBUG.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    phase = "collection"
    try:
        if arguments.command == "export":
            phase = "export"
            result = _export(arguments)
            report = {"run_id": arguments.run_id, "catalog_export": result}
            exit_code = 0 if result["status"] == "succeeded" else 1
        else:
            try:
                config = _config(arguments)
            except (KeyError, TypeError, ValueError) as error:
                raise RunSetupError(str(error)) from None
            report = run_catalog(arguments.database, arguments.run_id, config,
                                 resume=arguments.command == "resume",
                                 recovery_access_token=os.environ.get("SAFCO_RECOVERY_TOKEN"),
                                 **({"recover": True} if arguments.command == "recover" else {}))
            exit_code = 0 if report["status"] == "complete" else 1
            if arguments.output is not None:
                phase = "export"
                result = _export(arguments)
                report = report | {"catalog_export": result}
                if result["status"] == "failed":
                    exit_code = 1
        phase = "report"
        json.dump(report, sys.stdout, indent=2, allow_nan=False)
        sys.stdout.write("\n")
    except KeyboardInterrupt:
        hint = {"collection": "use resume with the same database and run ID",
                "export": "retry export with the same database and run ID",
                "report": "saved collection remains in SQLite; use export to save the catalog"}[phase]
        logger.warning("Command interrupted run_id=%s database=%s phase=%s; %s",
                       arguments.run_id, arguments.database, phase, hint)
        return 130
    except RunSetupError as error:
        logger.error("Cannot run run_id=%s: %s", arguments.run_id, error)
        return 2
    except (OSError, sqlite3.Error, RuntimeError) as error:
        hint = ("Another collector owns the checkpoint database" if isinstance(error, RuntimeError)
                and str(error).startswith("Another collector owns ") else
                "Check database access and saved progress before resuming")
        logger.error("Run stopped run_id=%s database=%s error_type=%s location=%s; %s",
                     arguments.run_id, arguments.database, type(error).__name__, failure_location(error), hint)
        return 1
    except Exception as error:
        logger.error("Unexpected failure run_id=%s error_type=%s location=%s; "
                     "saved progress remains in SQLite; use resume with the same database and run ID",
                     arguments.run_id, type(error).__name__, failure_location(error))
        json.dump({"run_id": arguments.run_id, "status": "interrupted", "stop_reason": "execution_error",
                   "diagnostic": failure_diagnostic(error), "next_action": "resume the saved run"}, sys.stdout)
        sys.stdout.write("\n")
        return 1
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
