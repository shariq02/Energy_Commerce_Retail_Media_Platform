"""Tool contracts: the change rules, the check, the lock writer, strict inputs.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

The change rules and the check run on small definitions built in the tests. The
repository tests read the real servers and `servers.yml` in this process, with no
subprocess and no Databricks.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import importlib
import json
import re
from pathlib import Path

import pytest
import yaml
from mcp.server.fastmcp.exceptions import ToolError

from ai.mcp_servers import contracts as ct
from ai.mcp_servers import contracts_write as cw
from ai.tools import common as cm

pytestmark = [pytest.mark.unit, pytest.mark.ai]

ROOT = Path(__file__).resolve().parents[2]


def definition():
    return {
        "name": "t",
        "description": "Does one thing for the caller.",
        "annotations": {"readOnlyHint": True, "openWorldHint": False},
        "inputSchema": {
            "type": "object",
            "title": "tArguments",
            "additionalProperties": False,
            "required": ["a"],
            "properties": {
                "a": {"type": "string", "title": "A"},
                "b": {"type": "integer", "default": 1, "title": "B"},
                "mode": {"type": "string", "enum": ["x", "y"], "title": "Mode"},
                "when": {
                    "anyOf": [{"type": "string", "format": "date"}, {"type": "null"}],
                    "default": None,
                    "title": "When",
                },
            },
        },
        "outputSchema": {
            "type": "object",
            "title": "R",
            "required": ["x", "inner"],
            "properties": {
                "x": {"type": "integer", "title": "X"},
                "y": {
                    "anyOf": [{"type": "string"}, {"type": "null"}],
                    "title": "Y",
                },
                "inner": {"$ref": "#/$defs/Inner"},
            },
            "$defs": {
                "Inner": {
                    "type": "object",
                    "title": "Inner",
                    "required": ["n"],
                    "properties": {"n": {"type": "number", "title": "N"}},
                }
            },
        },
    }


def changed(mutate):
    new = copy.deepcopy(definition())
    mutate(new)
    return ct.classify(definition(), new)


def inp(new):
    return new["inputSchema"]["properties"]


def out(new):
    return new["outputSchema"]["properties"]


# --- what breaks a caller -------------------------------------------------------


def test_the_same_definition_is_the_same():
    assert ct.classify(definition(), definition()) == ("same", [])


COMPATIBLE = {
    "tool description": lambda d: d.update(description="Does it in a new way."),
    "optional input added": lambda d: inp(d).update(c={"type": "string"}),
    "input enum value added": lambda d: inp(d)["mode"]["enum"].append("z"),
    "input made nullable": lambda d: inp(d).update(
        b={"anyOf": [{"type": "integer"}, {"type": "null"}], "title": "B"}
    ),
    "output property added": lambda d: out(d).update(z={"type": "string"}),
    "output made no longer nullable": lambda d: out(d).update(
        y={"type": "string", "title": "Y"}
    ),
    "property description": lambda d: inp(d)["a"].update(description="new words"),
    "nested property description": lambda d: d["outputSchema"]["$defs"]["Inner"][
        "properties"
    ]["n"].update(description="the n"),
}


@pytest.mark.parametrize("name", sorted(COMPATIBLE))
def test_a_compatible_change_is_compatible(name):
    kind, reasons = changed(COMPATIBLE[name])
    assert kind == "compatible" and reasons, name


BREAKING = {
    "required input added": lambda d: (
        inp(d).update(c={"type": "string"}),
        d["inputSchema"]["required"].append("c"),
    ),
    "input removed": lambda d: inp(d).pop("b"),
    "input type changed": lambda d: inp(d)["b"].update(type="string"),
    "optional input becomes required": lambda d: d["inputSchema"]["required"].append(
        "b"
    ),
    "input enum value removed": lambda d: inp(d)["mode"]["enum"].remove("y"),
    "input null no longer allowed": lambda d: inp(d).update(
        when={"type": "string", "format": "date", "title": "When"}
    ),
    "input format changed": lambda d: inp(d)["when"]["anyOf"][0].update(format="time"),
    "output now nullable": lambda d: out(d).update(
        x={"anyOf": [{"type": "integer"}, {"type": "null"}], "title": "X"}
    ),
    "output property removed": lambda d: out(d).pop("y"),
    "output type changed": lambda d: out(d)["x"].update(type="string"),
    "output no longer always present": lambda d: d["outputSchema"]["required"].remove(
        "x"
    ),
    "nested output type changed": lambda d: d["outputSchema"]["$defs"]["Inner"][
        "properties"
    ]["n"].update(type="string"),
    "annotations changed": lambda d: d["annotations"].update(readOnlyHint=False),
    "output schema removed": lambda d: d.pop("outputSchema"),
}


@pytest.mark.parametrize("name", sorted(BREAKING))
def test_a_breaking_change_is_breaking(name):
    kind, reasons = changed(BREAKING[name])
    assert kind == "breaking" and reasons, name


def test_a_schema_that_starts_to_refuse_extra_arguments_is_breaking():
    loose = definition()
    loose["inputSchema"].pop("additionalProperties")
    kind, reasons = ct.classify(loose, definition())
    assert kind == "breaking" and any("refused" in r for r in reasons)


def test_the_hash_ignores_the_order_of_keys():
    a = {"b": 1, "a": {"y": 2, "x": 3}}
    b = {"a": {"x": 3, "y": 2}, "b": 1}
    assert ct.canonical(a) == ct.canonical(b)
    assert ct.definition_hash(a) == hashlib.sha256(ct.canonical(a).encode()).hexdigest()


# --- the check ------------------------------------------------------------------


def contract(**kw):
    base = {
        "name": "t",
        "server": "s",
        "version": 1,
        "status": "active",
        "replaced_by": None,
        "errors": ("invalid_argument", "internal_error"),
        "changes": (
            {"date": "2026-10-28", "version": 1, "kind": "new", "note": "first"},
        ),
        "agents": ("a1", "a2"),
    }
    return ct.Contract(**{**base, **kw})


def code(defn=None):
    return {"t": {"server": "s", "definition": defn or definition()}}


def locked(defn=None, c=None):
    """The lock that the writer would make for the current code."""
    return ct.analyse(code(defn), {"t": c or contract()}, {"tools": {}}).lock


def check(defn=None, c=None, lock=None):
    return ct.analyse(
        code(defn), {"t": c or contract()}, lock if lock is not None else locked()
    )


def change(version, kind="compatible", note="changed"):
    return {"date": "2026-10-29", "version": version, "kind": kind, "note": note}


def test_a_new_tool_is_pending_and_then_ok_once_locked():
    first = ct.analyse(code(), {"t": contract()}, {"tools": {}})
    assert first.errors == [] and len(first.pending) == 1 and not first.ok
    assert check().ok


def test_an_unchanged_tool_is_ok():
    a = check()
    assert a.ok and a.errors == [] and a.pending == []


def test_a_new_tool_must_start_at_version_one():
    c = contract(version=2, changes=contract().changes + (change(2),))
    a = ct.analyse(code(), {"t": c}, {"tools": {}})
    assert any("starts at version 1" in e for e in a.errors)


def compatible_change(defn):
    defn["description"] = "Does one thing, and says so."


def test_a_compatible_change_with_a_higher_version_is_pending_and_noted():
    new = definition()
    compatible_change(new)
    c = contract(version=2, changes=contract().changes + (change(2, "description"),))
    a = ct.analyse(code(new), {"t": c}, locked())
    assert a.errors == [] and len(a.pending) == 1
    assert any("re-run the evaluation of a1, a2" in n for n in a.notes)


def test_a_compatible_change_without_a_higher_version_is_refused():
    new = definition()
    compatible_change(new)
    a = ct.analyse(code(new), {"t": contract()}, locked())
    assert any("without a higher version" in e for e in a.errors)


def test_the_last_change_must_say_compatible_or_description():
    new = definition()
    compatible_change(new)
    c = contract(version=2, changes=contract().changes + (change(2, "new"),))
    a = ct.analyse(code(new), {"t": c}, locked())
    assert any("kind compatible or description" in e for e in a.errors)


def test_a_breaking_change_is_refused_even_with_a_higher_version():
    new = definition()
    new["inputSchema"]["properties"].pop("b")
    c = contract(version=2, changes=contract().changes + (change(2),))
    a = ct.analyse(code(new), {"t": c}, locked())
    assert any("breaking change" in e and "new tool name" in e for e in a.errors)


def test_a_raised_version_without_a_change_is_pending_a_lower_one_is_an_error():
    up = contract(version=2, changes=contract().changes + (change(2),))
    a = ct.analyse(code(), {"t": up}, locked())
    assert a.errors == [] and any("without a change" in p for p in a.pending)
    lock = locked(c=up)
    assert any("fell" in e for e in ct.analyse(code(), {"t": contract()}, lock).errors)


def test_a_lock_entry_that_was_edited_by_hand_is_refused():
    lock = locked()
    lock["tools"]["t"]["definition"]["description"] = "edited"
    assert any("own hash" in e for e in check(lock=lock).errors)


def test_a_removed_tool_is_pending_and_needs_approval():
    a = ct.analyse({}, {}, locked())
    assert a.errors == [] and any("owner's approval" in p for p in a.pending)
    assert a.lock["tools"] == {}


def test_a_tool_without_a_contract_or_without_code_is_an_error():
    only_code = ct.analyse(code(), {}, {"tools": {}})
    only_contract = ct.analyse({}, {"t": contract()}, {"tools": {}})
    assert any("no contract" in e for e in only_code.errors)
    assert any("no server declares" in e for e in only_contract.errors)


def test_a_tool_on_another_server_than_its_contract_is_an_error():
    other = {"t": {"server": "x", "definition": definition()}}
    a = ct.analyse(other, {"t": contract()}, {"tools": {}})
    assert any("declared on x" in e for e in a.errors)


def test_deprecating_needs_the_deprecated_change_and_cannot_be_undone():
    old = contract()
    dep = contract(
        version=2,
        status="deprecated",
        replaced_by="u",
        changes=old.changes + (change(2, "deprecated"),),
    )
    lock = locked(c=old)
    a = ct.analyse(code(), {"t": dep}, lock)
    assert a.errors == [] and any("now deprecated" in p for p in a.pending)
    plain = contract(
        version=2,
        status="deprecated",
        replaced_by="u",
        changes=old.changes + (change(2, "compatible"),),
    )
    assert any(
        "kind deprecated" in e for e in ct.analyse(code(), {"t": plain}, lock).errors
    )
    back = ct.analyse(
        code(),
        {"t": contract(version=3, changes=dep.changes + (change(3),))},
        locked(c=dep),
    )
    assert any("cannot become active" in e for e in back.errors)


def test_the_evaluation_record_is_kept_when_the_lock_is_made_again():
    lock = locked()
    lock["tools"]["t"]["evaluated_version"] = {"a1": 1, "a2": None}
    new = definition()
    compatible_change(new)
    c = contract(version=2, changes=contract().changes + (change(2, "description"),))
    entry = ct.analyse(code(new), {"t": c}, lock).lock["tools"]["t"]
    assert entry["evaluated_version"] == {"a1": 1, "a2": None}
    assert entry["version"] == 2


def test_the_lock_text_is_sorted_and_reads_back():
    text = ct.render_lock(locked())
    assert text.endswith("\n") and json.loads(text)["format"] == ct.LOCK_FORMAT
    assert text == ct.render_lock(json.loads(text))


# --- the part a person writes ---------------------------------------------------

GOOD = """
servers:
  s:
    allowed_agents: [a1]
    tools:
      t:
        version: 1
        status: active
        replaced_by: null
        errors: [invalid_argument, internal_error]
        changes:
          - {date: "2026-10-28", version: 1, kind: new, note: first}
"""


def load(tmp_path, text):
    path = tmp_path / "servers.yml"
    path.write_text(text, encoding="utf-8")
    return ct.load_contracts(path)


def test_a_clean_contract_file_loads(tmp_path):
    contracts, problems = load(tmp_path, GOOD)
    assert problems == [] and contracts["t"].agents == ("a1",)


@pytest.mark.parametrize(
    "old, new, fragment",
    [
        ("version: 1\n        status", "version: 0\n        status", "version must"),
        ("status: active", "status: maybe", "status must"),
        ("[invalid_argument, internal_error]", "[invalid_argument]", "internal_error"),
        (
            "[invalid_argument, internal_error]",
            "[made_up, internal_error]",
            "unknown error",
        ),
        ('date: "2026-10-28"', 'date: "28 Oct"', "no date"),
        ("kind: new", "kind: odd", "change kind"),
        ("note: first", "note: ''", "no note"),
        ("kind: new", "kind: compatible", "first change"),
        ("replaced_by: null", "replaced_by: u", "no replaced_by"),
        ("status: active", "status: deprecated", "names replaced_by"),
    ],
)
def test_a_bad_contract_is_reported(tmp_path, old, new, fragment):
    _, problems = load(tmp_path, GOOD.replace(old, new))
    assert any(fragment in p for p in problems), problems


def test_the_last_change_must_be_the_current_version(tmp_path):
    _, problems = load(
        tmp_path,
        GOOD.replace("version: 1\n        status", "version: 2\n        status"),
    )
    assert any("current version" in p for p in problems)


def test_a_replaced_by_must_name_an_active_tool(tmp_path):
    text = GOOD.replace("status: active", "status: deprecated").replace(
        "replaced_by: null", "replaced_by: nothing"
    )
    _, problems = load(tmp_path, text)
    assert any("not an active tool" in p for p in problems)


def test_a_list_of_tool_names_is_not_a_contract(tmp_path):
    _, problems = load(tmp_path, "servers:\n  s:\n    tools: [a, b]\n")
    assert any("mapping" in p for p in problems)


# --- the real servers -----------------------------------------------------------


def test_the_servers_file_has_a_clean_contract_for_each_of_the_fourteen_tools():
    contracts, problems = ct.load_contracts()
    assert problems == [] and len(contracts) == 14
    assert {c.version for c in contracts.values()} == {1}
    assert {c.status for c in contracts.values()} == {"active"}


def test_the_definitions_come_from_the_servers_with_the_listed_keys():
    found = ct.current_definitions()
    contracts, _ = ct.load_contracts()
    assert set(found) == set(contracts)
    for name, item in found.items():
        assert item["server"] == contracts[name].server
        keys = set(item["definition"])
        assert {"name", "description", "inputSchema", "outputSchema"} <= keys, name
        assert "annotations" in keys, name


def test_every_input_schema_refuses_extra_arguments():
    for name, item in ct.current_definitions().items():
        schema = item["definition"]["inputSchema"]
        assert schema.get("additionalProperties") is False, name


def test_a_call_with_an_unknown_argument_fails_before_the_tool_runs():
    module = importlib.import_module("ai.mcp_servers.energy_tools")
    with pytest.raises(ToolError, match="Extra inputs are not permitted"):
        asyncio.run(
            module.app.call_tool("energy_get_metric", {"metric_key": "x", "z": 1})
        )


MODULES = {
    "energy_tools": ("energy", "metrics"),
    "commerce_tools": ("commerce", "metrics"),
    "ml_tools": ("ml",),
    "dq_tools": ("dq",),
    "knowledge_tools": ("knowledge",),
}


def test_the_declared_error_codes_cover_the_codes_the_modules_raise():
    contracts, _ = ct.load_contracts()
    pattern = re.compile(r'ToolFailure\(\s*"(\w+)"')
    for server, modules in MODULES.items():
        declared = set()
        for c in contracts.values():
            if c.server == server:
                declared |= set(c.errors)
        raised = set()
        for module in modules:
            text = (ROOT / "ai" / "tools" / f"{module}.py").read_text(encoding="utf-8")
            raised |= set(pattern.findall(text))
        assert raised <= declared, (server, raised - declared)
        assert declared <= set(cm.ERROR_CODES)


def test_the_lock_matches_the_servers_when_it_exists():
    if not ct.LOCK_FILE.exists():
        pytest.skip("no lock yet: run python3 -m ai.mcp_servers.contracts_write")
    analysis = ct.analyse_repository()
    assert analysis.errors == [] and analysis.pending == [], (
        analysis.errors,
        analysis.pending,
    )


def test_the_servers_file_is_the_only_place_that_lists_versions():
    text = (ROOT / "ai" / "mcp_servers" / "servers.yml").read_text(encoding="utf-8")
    assert len(re.findall(r"^\s+version: \d+$", text, re.MULTILINE)) == 14
    assert yaml.safe_load(text)["servers"]["cross_ecosystem_tools"]["tools"] == []


# --- the writer -----------------------------------------------------------------


def stub(monkeypatch, tmp_path, analysis):
    lock = tmp_path / "tools.lock.json"
    monkeypatch.setattr(ct, "LOCK_FILE", lock)
    monkeypatch.setattr(ct, "analyse_repository", lambda: analysis)
    return lock


def pending_analysis():
    a = ct.analyse(code(), {"t": contract()}, {"tools": {}})
    return a


@pytest.mark.parametrize("answer", ["y", "yes", " YES "])
def test_the_writer_writes_after_a_yes(monkeypatch, tmp_path, answer):
    lock = stub(monkeypatch, tmp_path, pending_analysis())
    assert cw.main(lambda _: answer) == 0
    assert json.loads(lock.read_text(encoding="utf-8"))["tools"]["t"]["version"] == 1


@pytest.mark.parametrize("answer", ["", "n", "no", "maybe"])
def test_the_writer_writes_nothing_after_another_answer(monkeypatch, tmp_path, answer):
    lock = stub(monkeypatch, tmp_path, pending_analysis())
    assert cw.main(lambda _: answer) == 1 and not lock.exists()


def test_the_writer_refuses_while_the_check_has_an_error(monkeypatch, tmp_path):
    analysis = pending_analysis()
    analysis.errors.append("t: something is wrong")
    lock = stub(monkeypatch, tmp_path, analysis)
    assert cw.main(lambda _: "yes") == 1 and not lock.exists()


def test_the_writer_does_nothing_when_the_lock_is_current(monkeypatch, tmp_path):
    lock = stub(monkeypatch, tmp_path, check())

    def never(_):
        raise AssertionError("no question when nothing changes")

    assert cw.main(never) == 0 and not lock.exists()
