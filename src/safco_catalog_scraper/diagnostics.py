"""Small failure diagnostics without exception messages, source text or local values."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any


def failure_diagnostic(error: BaseException) -> dict[str, Any]:
    """Return the exception type and at most five package stack locations.

    Inspect code metadata only. Never include exception text, argument values,
    source lines, absolute paths or frame locals in logs or reports.
    """
    locations = []
    trace = error.__traceback__
    package = Path(__file__).parent
    while trace is not None:
        code = trace.tb_frame.f_code
        path = Path(code.co_filename)
        if path.parent == package:
            locations.append({"file": path.name,
                              "function": re.sub(r"[^A-Za-z0-9_<>]", "", code.co_name)[:80],
                              "line": trace.tb_lineno})
        trace = trace.tb_next
    return {"error_type": re.sub(r"[^A-Za-z0-9_]", "", type(error).__name__)[:80],
            "locations": locations[-5:]}


def failure_location(error: BaseException) -> str:
    """Format a safe failure location for a short console message."""
    detail = failure_diagnostic(error)
    frames = detail["locations"]
    return (" -> ".join(f"{frame['file']}:{frame['line']} ({frame['function']})" for frame in frames)
            if frames else "location unavailable")
