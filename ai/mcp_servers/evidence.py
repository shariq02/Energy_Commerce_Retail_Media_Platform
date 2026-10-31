"""The result of one tool call as the caller sees it: status, data, error, provenance.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

`ToolEvidence` is what `invoke_tool` returns. It never holds a raw exception text.
A tool error arrives as a typed payload (code, message, hint); any other error text
is replaced by a fixed message, and all text passes the redaction of the tool
library. Fields that carry source text come back inside a marked data block, so a
graph inserts them as data and never as an instruction. Library only.
"""

from __future__ import annotations

import json
import re
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from ai.tools import common as cm

Status = Literal[
    "ok",
    "tool_error",
    "invalid_arguments",
    "denied",
    "timeout",
    "server_failed",
    "contract_violation",
    "budget_exhausted",
]
STATUSES = (
    "ok",
    "tool_error",
    "invalid_arguments",
    "denied",
    "timeout",
    "server_failed",
    "contract_violation",
    "budget_exhausted",
)
MESSAGE_LIMIT = 200
ERROR_TEXT_LIMIT = 20_000
BLOCK_TAG = "untrusted-data"
# The text of a typed tool error is exactly this: the server wrapper, a name, the JSON.
_PAYLOAD = re.compile(r'\AError executing tool \w+: (\{"code":.*\})\s*\Z', re.DOTALL)


class EvidenceError(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: str
    message: str
    hint: str = ""


class ToolEvidence(BaseModel):
    """One call: the status, the data of the tool and where the data comes from."""

    model_config = ConfigDict(extra="forbid")

    status: Status
    tool: str
    server: str | None = None
    tool_version: int | None = None
    definition_hash: str | None = None
    data: dict[str, Any] | None = None
    error: EvidenceError | None = None
    retry_attempt: int = 0
    duration_ms: int = 0
    as_of: str | None = None
    trust: cm.Trust | None = None
    truncated: bool = False
    row_count: int | None = None
    untrusted_fields: list[str] = Field(default_factory=list)
    log_written: bool = True


def safe_text(value: object, limit: int = MESSAGE_LIMIT) -> str:
    """Text for a message: secrets removed, control characters removed, cut."""
    return cm.clean_text(cm.redact(str(value)), limit)


def make_error(code: str, message: str, hint: str = "") -> EvidenceError:
    return EvidenceError(code=code, message=safe_text(message), hint=safe_text(hint))


def parse_error(text: str) -> tuple[Status, EvidenceError]:
    """(status, error) of an error result text of a tool server."""
    text = text[:ERROR_TEXT_LIMIT]
    match = _PAYLOAD.match(text)
    if match:
        try:
            payload = json.loads(match.group(1))
        except (ValueError, RecursionError):
            payload = None
        if isinstance(payload, dict) and payload.get("code") in cm.ERROR_CODES:
            code = payload["code"]
            message, hint = payload.get("message", ""), payload.get("hint", "")
            status = "invalid_arguments" if code == "invalid_argument" else "tool_error"
            return status, make_error(code, message, hint)
    lowered = text.lower()
    if "output validation error" in lowered:
        return "contract_violation", make_error(
            "contract_violation", "the result does not match the output schema"
        )
    if "validation error" in lowered:
        return "invalid_arguments", make_error(
            "invalid_argument",
            "the arguments do not match the input schema",
            "check the names, types and values in the input schema of the tool",
        )
    return "tool_error", make_error(
        "internal_error", "the tool failed", "the detail is in the server log"
    )


def check_result(structured: Any, needs_trust: bool) -> EvidenceError | None:
    """None when the structured result is usable, else the contract violation."""
    if not isinstance(structured, dict):
        return make_error("contract_violation", "the result has no structured content")
    if needs_trust:
        try:
            cm.Trust.model_validate(structured.get("trust"))
        except ValueError:
            return make_error(
                "contract_violation", "the result has no valid trust value"
            )
    return None


def _block(tool: str, field: str, value: Any) -> str:
    """The value as text: no angle brackets, no control characters, cut to its limit,
    so the text cannot open or close a block."""
    limit = cm.UNTRUSTED_LIMITS.get((tool, field), cm.TEXT_LIMIT)
    text = cm.clean_text(str(value).replace("<", " ").replace(">", " "), limit)
    return f'<{BLOCK_TAG} tool="{tool}" field="{field}">{text}</{BLOCK_TAG}>'


def wrap_untrusted(tool: str, data: Any) -> tuple[Any, list[str]]:
    """The data with each untrusted text field inside a marked block, and the names
    of the fields that were found. The input is not changed."""
    fields = set(cm.UNTRUSTED_FIELDS.get(tool, ()))
    found: set[str] = set()

    def walk(node: Any) -> Any:
        if isinstance(node, dict):
            out = {}
            for key, value in node.items():
                if key in fields and value is not None:
                    found.add(key)
                    out[key] = _block(tool, key, value)
                else:
                    out[key] = walk(value)
            return out
        if isinstance(node, list):
            return [walk(item) for item in node]
        return node

    return walk(data), sorted(found)
