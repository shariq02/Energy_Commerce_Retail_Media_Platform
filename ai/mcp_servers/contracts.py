"""Tool contracts: the definitions, the lock file, the change rules and the check.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

The definition of a tool (name, description, input schema, output schema, annotations)
comes from the server code, in this process, with no subprocess and no Databricks.
`servers.yml` holds the part that a person writes: version, status, error codes and
the list of changes. `tools.lock.json` holds the generated definitions and their
SHA-256. The check compares the code with the lock:

- a definition that changed in a way that breaks a caller is refused on the same
  name: it must be a new tool name, and the old tool is marked deprecated;
- a compatible change (an optional input or output field, a new error code, a
  changed description) needs a higher version and a line in `changes`;
- a new tool starts at version 1; a removed tool needs the owner's approval, which
  the writer asks for.

The rules for what breaks a caller are ours, not MCP rules.

Usage (repository root):
    python3 -m ai.mcp_servers.contracts
"""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from ai.tools import common as cm

LOCK_FILE = Path(__file__).with_name("tools.lock.json")
LOCK_FORMAT = 1
STATUSES = ("active", "deprecated")
KINDS = ("new", "compatible", "description", "deprecated")
DEFINITION_KEYS = (
    "name",
    "title",
    "description",
    "inputSchema",
    "outputSchema",
    "annotations",
)
CONSTRAINTS = (
    "format",
    "minimum",
    "maximum",
    "exclusiveMinimum",
    "exclusiveMaximum",
    "minLength",
    "maxLength",
    "pattern",
    "minItems",
    "maxItems",
    "const",
)
NOTE_KEYS = ("title", "description", "default", "examples")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


# --- definitions and hashes -----------------------------------------------------


def canonical(definition: dict) -> str:
    return json.dumps(
        definition, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )


def definition_hash(definition: dict) -> str:
    return hashlib.sha256(canonical(definition).encode("utf-8")).hexdigest()


def current_definitions() -> dict[str, dict]:
    """Tool name -> {server, definition}, from the server modules of this process."""
    found: dict[str, dict] = {}
    for server in cm.load_scopes():
        module = importlib.import_module(f"ai.mcp_servers.{server}")
        for tool in asyncio.run(module.app.list_tools()):
            dumped = tool.model_dump(mode="json", exclude_none=True)
            found[tool.name] = {
                "server": server,
                "definition": {k: dumped[k] for k in DEFINITION_KEYS if k in dumped},
            }
    return found


# --- the part a person writes ---------------------------------------------------


@dataclass(frozen=True)
class Contract:
    name: str
    server: str
    version: int
    status: str
    replaced_by: str | None
    errors: tuple[str, ...]
    changes: tuple[dict, ...]
    agents: tuple[str, ...]


def _contract(server: str, name: str, raw: dict, agents: tuple[str, ...]):
    """(Contract, problems) of one tool entry."""
    problems: list[str] = []
    version, status = raw.get("version"), raw.get("status")
    errors, changes = raw.get("errors") or [], raw.get("changes") or []
    if not isinstance(version, int) or isinstance(version, bool) or version < 1:
        problems.append(f"{name}: version must be a whole number from 1")
    if status not in STATUSES:
        problems.append(f"{name}: status must be one of {STATUSES}")
    if bad := sorted(set(errors) - set(cm.ERROR_CODES)):
        problems.append(f"{name}: unknown error codes {bad}")
    if "internal_error" not in errors:
        problems.append(f"{name}: errors must list internal_error")
    seen = 0
    for change in changes:
        number = change.get("version")
        if not _DATE.match(str(change.get("date", ""))):
            problems.append(f"{name}: a change has no date like 2026-10-28")
        if change.get("kind") not in KINDS:
            problems.append(f"{name}: a change kind must be one of {KINDS}")
        if not str(change.get("note", "")).strip():
            problems.append(f"{name}: a change has no note")
        if not isinstance(number, int) or number <= seen:
            problems.append(f"{name}: change versions must rise")
        seen = number if isinstance(number, int) else seen
    if not changes:
        problems.append(f"{name}: the list of changes is empty")
    else:
        if changes[0].get("kind") != "new" or changes[0].get("version") != 1:
            problems.append(f"{name}: the first change must be version 1, kind new")
        if changes[-1].get("version") != version:
            problems.append(f"{name}: the last change must be the current version")
    replaced = raw.get("replaced_by")
    if status == "deprecated" and not replaced:
        problems.append(f"{name}: a deprecated tool names replaced_by")
    if status == "active" and replaced:
        problems.append(f"{name}: an active tool has no replaced_by")
    contract = Contract(
        name=name,
        server=server,
        version=version if isinstance(version, int) else 0,
        status=str(status),
        replaced_by=replaced,
        errors=tuple(errors),
        changes=tuple(changes),
        agents=agents,
    )
    return contract, problems


def load_contracts(
    path: Path = cm.SERVERS_FILE,
) -> tuple[dict[str, Contract], list[str]]:
    """Tool name -> Contract, and the problems of the file."""
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    contracts: dict[str, Contract] = {}
    problems: list[str] = []
    for server, item in data["servers"].items():
        tools = item.get("tools") or {}
        if not isinstance(tools, dict):
            problems.append(f"{server}: tools must be a mapping of name to contract")
            continue
        agents = tuple(item.get("allowed_agents", []))
        for name, raw in tools.items():
            contract, found = _contract(server, name, raw or {}, agents)
            contracts[name] = contract
            problems += found
    for contract in contracts.values():
        target = contract.replaced_by
        if target and (target not in contracts or contracts[target].status != "active"):
            problems.append(
                f"{contract.name}: replaced_by {target} is not an active tool"
            )
    return contracts, problems


# --- what breaks a caller -------------------------------------------------------


@dataclass
class Found:
    breaking: list[str] = field(default_factory=list)
    compatible: list[str] = field(default_factory=list)


def _resolve(node, defs: dict, depth: int = 0):
    if isinstance(node, dict):
        if "$ref" in node and depth < 12:
            target = defs.get(node["$ref"].rsplit("/", 1)[-1], {})
            merged = {**target, **{k: v for k, v in node.items() if k != "$ref"}}
            return _resolve(merged, defs, depth + 1)
        return {k: _resolve(v, defs, depth) for k, v in node.items() if k != "$defs"}
    if isinstance(node, list):
        return [_resolve(v, defs, depth) for v in node]
    return node


def _unwrap(node: dict) -> tuple[dict, bool]:
    """A nullable type: (the inner schema, True)."""
    members = node.get("anyOf") or []
    plain = [m for m in members if m.get("type") != "null"]
    if len(members) == 2 and len(plain) == 1:
        outer = {k: v for k, v in node.items() if k != "anyOf"}
        return {**outer, **plain[0]}, True
    return node, False


def _strip(node):
    if isinstance(node, dict):
        return {k: _strip(v) for k, v in node.items() if k not in NOTE_KEYS}
    if isinstance(node, list):
        return [_strip(v) for v in node]
    return node


def _choices(node: dict) -> list[str]:
    return sorted(json.dumps(_strip(m), sort_keys=True) for m in node["anyOf"])


def _compare_object(old: dict, new: dict, direction: str, path: str, found: Found):
    old_props, new_props = old.get("properties", {}), new.get("properties", {})
    old_req, new_req = set(old.get("required", [])), set(new.get("required", []))
    for key, schema in old_props.items():
        if key in new_props:
            _compare(schema, new_props[key], direction, f"{path}.{key}", found)
        else:
            found.breaking.append(f"{path}.{key}: removed")
    for key in sorted(new_props.keys() - old_props.keys()):
        if direction == "input" and key in new_req:
            found.breaking.append(f"{path}.{key}: new required input")
        else:
            found.compatible.append(f"{path}.{key}: added")
    if direction == "input":
        for key in sorted((new_req - old_req) & old_props.keys()):
            found.breaking.append(f"{path}.{key}: now required")
    else:
        for key in sorted((old_req - new_req) & new_props.keys()):
            found.breaking.append(f"{path}.{key}: no longer always present")
    extra_old, extra_new = (
        old.get("additionalProperties"),
        new.get("additionalProperties"),
    )
    if extra_old != extra_new:
        if isinstance(extra_old, dict) and isinstance(extra_new, dict):
            _compare(extra_old, extra_new, direction, f"{path}{{}}", found)
        elif direction == "input" and extra_new is False:
            found.breaking.append(f"{path}: extra arguments are now refused")
        else:
            found.compatible.append(f"{path}: additionalProperties changed")


def _compare(old: dict, new: dict, direction: str, path: str, found: Found) -> None:
    old, old_null = _unwrap(old)
    new, new_null = _unwrap(new)
    if old_null != new_null:
        widened = new_null
        breaks = (not widened) if direction == "input" else widened
        text = "null is now allowed" if widened else "null is no longer allowed"
        (found.breaking if breaks else found.compatible).append(f"{path}: {text}")
    if (old.get("description"), old.get("title")) != (
        new.get("description"),
        new.get("title"),
    ):
        found.compatible.append(f"{path}: description changed")
    if old.get("type") != new.get("type") or ("anyOf" in old) != ("anyOf" in new):
        found.breaking.append(f"{path}: type changed")
        return
    if "anyOf" in old:
        if _choices(old) != _choices(new):
            found.breaking.append(f"{path}: the allowed types changed")
        return
    for key in CONSTRAINTS:
        if old.get(key) != new.get(key):
            found.breaking.append(f"{path}: {key} changed")
    old_enum, new_enum = (
        set(map(str, old.get("enum", []))),
        set(map(str, new.get("enum", []))),
    )
    if direction == "input" and old_enum - new_enum:
        found.breaking.append(f"{path}: values removed {sorted(old_enum - new_enum)}")
    elif old_enum != new_enum:
        found.compatible.append(f"{path}: the values changed")
    if old.get("type") == "object":
        _compare_object(old, new, direction, path, found)
    elif old.get("type") == "array":
        _compare(
            old.get("items", {}), new.get("items", {}), direction, f"{path}[]", found
        )


def classify(old: dict, new: dict) -> tuple[str, list[str]]:
    """('same' | 'compatible' | 'breaking', the reasons) of a change of definition."""
    if canonical(old) == canonical(new):
        return "same", []
    found = Found()
    if old.get("annotations") != new.get("annotations"):
        found.breaking.append("annotations changed")
    if old.get("description") != new.get("description"):
        found.compatible.append("tool description changed")
    for key, direction in (("inputSchema", "input"), ("outputSchema", "output")):
        before, after = old.get(key), new.get(key)
        if before is None and after is None:
            continue
        if before is None or after is None:
            found.breaking.append(f"{key} was added or removed")
            continue
        _compare(
            _resolve(before, before.get("$defs", {})),
            _resolve(after, after.get("$defs", {})),
            direction,
            direction,
            found,
        )
    if found.breaking:
        return "breaking", found.breaking
    return "compatible", found.compatible or [
        "the definition differs, the meaning does not"
    ]


# --- the lock and the check -----------------------------------------------------


@dataclass
class Analysis:
    errors: list[str] = field(default_factory=list)
    pending: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    lock: dict = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return not self.errors and not self.pending


def read_lock(path: Path = LOCK_FILE) -> dict:
    if not path.exists():
        return {"format": LOCK_FORMAT, "tools": {}}
    return json.loads(path.read_text(encoding="utf-8"))


def _entry(code: dict, contract: Contract, old: dict | None) -> dict:
    before = (old or {}).get("evaluated_version", {})
    return {
        "server": code["server"],
        "version": contract.version,
        "status": contract.status,
        "definition_hash": definition_hash(code["definition"]),
        "definition": code["definition"],
        "evaluated_version": {agent: before.get(agent) for agent in contract.agents},
    }


def _judge_change(a: Analysis, name: str, contract: Contract, old: dict, code: dict):
    kind, reasons = classify(old["definition"], code["definition"])
    text = "; ".join(reasons)
    if kind == "breaking":
        a.errors.append(
            f"{name}: breaking change ({text}): publish a new tool name and mark "
            "this one deprecated"
        )
        return
    if contract.version <= old["version"]:
        a.errors.append(f"{name}: changed ({text}) without a higher version")
        return
    if contract.changes[-1]["kind"] not in ("compatible", "description"):
        a.errors.append(
            f"{name}: the last change must be kind compatible or description"
        )
        return
    a.pending.append(f"{name}: version {old['version']} to {contract.version} ({text})")
    a.notes.append(f"{name}: re-run the evaluation of {', '.join(contract.agents)}")


def _judge_same(a: Analysis, name: str, contract: Contract, old: dict) -> None:
    if contract.version < old["version"]:
        a.errors.append(f"{name}: the version fell from {old['version']}")
    elif contract.version > old["version"]:
        a.pending.append(
            f"{name}: version raised to {contract.version} without a change"
        )
    if contract.status != old["status"]:
        if old["status"] == "deprecated":
            a.errors.append(f"{name}: a deprecated tool cannot become active")
        elif contract.changes[-1]["kind"] != "deprecated":
            a.errors.append(
                f"{name}: the last change of a deprecated tool is kind deprecated"
            )
        else:
            a.pending.append(
                f"{name}: now deprecated, replaced by {contract.replaced_by}"
            )


def analyse(
    code: dict[str, dict],
    contracts: dict[str, Contract],
    lock: dict,
    file_problems: list[str] | None = None,
) -> Analysis:
    a = Analysis(errors=list(file_problems or []))
    old_tools = lock.get("tools", {})
    entries: dict[str, dict] = {}
    for name in sorted(set(code) | set(contracts)):
        tool, contract, old = code.get(name), contracts.get(name), old_tools.get(name)
        if contract is None:
            a.errors.append(f"{name}: the server has it, servers.yml has no contract")
            continue
        if tool is None:
            a.errors.append(f"{name}: servers.yml has it, no server declares it")
            continue
        if tool["server"] != contract.server:
            a.errors.append(
                f"{name}: declared on {tool['server']}, listed on {contract.server}"
            )
            continue
        entries[name] = _entry(tool, contract, old)
        if old is None:
            a.pending.append(f"{name}: new tool, version {contract.version}")
            if contract.version != 1:
                a.errors.append(f"{name}: a new tool starts at version 1")
        elif definition_hash(old["definition"]) != old["definition_hash"]:
            a.errors.append(f"{name}: the lock entry does not match its own hash")
        elif old["definition_hash"] == entries[name]["definition_hash"]:
            _judge_same(a, name, contract, old)
        else:
            _judge_change(a, name, contract, old, tool)
    for name in sorted(set(old_tools) - set(code) - set(contracts)):
        a.pending.append(f"{name}: removed (needs the owner's approval)")
    waiting = [
        n
        for n, e in entries.items()
        if any(v != e["version"] for v in e["evaluated_version"].values())
    ]
    if waiting:
        a.notes.append(f"{len(waiting)} tool(s) have no recorded evaluation yet")
    a.lock = {"format": LOCK_FORMAT, "tools": entries}
    return a


def analyse_repository() -> Analysis:
    contracts, problems = load_contracts()
    return analyse(current_definitions(), contracts, read_lock(), problems)


def render_lock(lock: dict) -> str:
    return json.dumps(lock, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def main() -> int:
    a = analyse_repository()
    print(f"tool contracts: {len(a.lock['tools'])} tool(s), lock {LOCK_FILE.name}")
    for text in a.errors:
        print("ERROR  ", text)
    for text in a.pending:
        print("PENDING", text)
    for text in a.notes:
        print("NOTE   ", text)
    if a.ok:
        print("OK: the code matches the lock")
        return 0
    if a.errors:
        print("STOP: fix the errors above")
    else:
        print(
            "The lock is behind the code: run python3 -m ai.mcp_servers.contracts_write"
        )
    return 1


if __name__ == "__main__":
    sys.exit(main())
