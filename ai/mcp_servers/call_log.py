"""The tool call log: one JSON line per attempt, one file per day.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Only the invocation layer (`invoke.py`) writes this log, because only the client sees
a timeout, a crash or a denied start. A line holds the identity of the call, the
status of the attempt and the canonical arguments cut to 500 characters. An argument
that takes free text is logged as a hash only. A result is never stored. Lines are
appended; nothing is changed or deleted. `tool_call_log.schema.json` describes a
line. Library only.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai.tools import common as cm

REPO_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = REPO_ROOT / "logs" / "tool_calls"
SCHEMA_FILE = Path(__file__).with_name("tool_call_log.schema.json")
ARGUMENT_LIMIT = 500
NAME_LIMIT = 64
_NAME_OUTSIDE = re.compile(r"[^A-Za-z0-9_]")
FREE_TEXT_MARK = "[free text, hash only]"


def canonical(value: Any) -> str:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str
    )


def safe_name(name: object) -> str:
    """A name for a log line or an evidence: word characters only, at most 64."""
    return _NAME_OUTSIDE.sub("_", str(name))[:NAME_LIMIT] or "_"


def argument_hash(arguments: dict[str, Any]) -> str:
    return hashlib.sha256(canonical(arguments).encode("utf-8")).hexdigest()


def logged_arguments(tool: str, arguments: dict[str, Any]) -> str:
    """The canonical arguments for the log: free-text values replaced, cut to 500."""
    free = cm.FREE_TEXT_ARGUMENTS.get(tool, ())
    shown = {k: (FREE_TEXT_MARK if k in free else v) for k, v in arguments.items()}
    return canonical(shown)[:ARGUMENT_LIMIT]


@dataclass(frozen=True)
class LogLine:
    trace_id: str
    timestamp: str
    agent: str
    mcp_server: str
    tool_name: str
    argument_hash: str
    arguments: str
    result_status: str
    duration_ms: int
    retry_attempt: int
    tool_version: int | None
    definition_hash: str | None
    error_code: str | None
    row_count: int | None


def now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds")


def log_path(directory: Path, stamp: str) -> Path:
    return directory / f"tool_calls_{stamp[:10]}.jsonl"


def write_line(line: LogLine, directory: Path = LOG_DIR) -> Path:
    """Append one line to the file of the day of the line; returns the file."""
    directory.mkdir(parents=True, exist_ok=True)
    path = log_path(directory, line.timestamp)
    text = cm.redact(canonical(asdict(line)))
    with path.open("a", encoding="utf-8") as handle:
        handle.write(text + "\n")
    return path
