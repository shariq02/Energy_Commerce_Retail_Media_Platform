"""The six tool servers: tools against servers.yml, annotations, scope, stdout.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

The servers are loaded in the test process and their tool lists are read without a
subprocess and without Databricks. The source checks read the files of
`ai/mcp_servers/` and `ai/tools/`.
"""

from __future__ import annotations

import asyncio
import importlib
import re
from pathlib import Path

import pytest

from ai.mcp_servers import check
from ai.tools import common as cm

pytestmark = [pytest.mark.unit, pytest.mark.ai]

ROOT = Path(__file__).resolve().parents[2]
SERVER_DIR = ROOT / "ai" / "mcp_servers"
TOOL_DIR = ROOT / "ai" / "tools"
SCOPES = cm.load_scopes()
SERVER_FILES = sorted(p for p in SERVER_DIR.glob("*_tools.py"))
TOOL_FILES = sorted(TOOL_DIR.glob("*.py"))


def listed(server: str):
    module = importlib.import_module(f"ai.mcp_servers.{server}")
    return asyncio.run(module.app.list_tools())


def test_every_server_file_has_an_entry_in_servers_yml():
    assert {p.stem for p in SERVER_FILES} == set(SCOPES)


@pytest.mark.parametrize("server", sorted(SCOPES))
def test_the_tools_of_a_server_are_the_listed_ones_sorted_by_name(server):
    names = [tool.name for tool in listed(server)]
    assert names == list(SCOPES[server].tools)
    assert names == sorted(names)


@pytest.mark.parametrize("server", sorted(SCOPES))
def test_every_tool_is_read_only_and_closed_world_with_a_description(server):
    for tool in listed(server):
        assert tool.annotations.readOnlyHint is True, tool.name
        assert tool.annotations.openWorldHint is False, tool.name
        assert tool.description and len(tool.description) > 60, tool.name


@pytest.mark.parametrize("server", sorted(SCOPES))
def test_every_tool_has_a_typed_output_schema(server):
    for tool in listed(server):
        assert tool.outputSchema and tool.outputSchema.get("type") == "object"
        props = tool.outputSchema["properties"]
        assert {"source_tables", "row_count", "warnings"} <= set(props), tool.name


def test_the_domain_tools_return_a_trust_value_and_the_others_do_not():
    for server in ("energy_tools", "commerce_tools"):
        for tool in listed(server):
            assert "trust" in tool.outputSchema["properties"], tool.name
    for server in ("ml_tools", "dq_tools", "knowledge_tools"):
        for tool in listed(server):
            assert "trust" not in tool.outputSchema["properties"], tool.name


def test_the_cross_ecosystem_server_has_no_tool():
    assert listed("cross_ecosystem_tools") == []


def test_the_smoke_check_calls_exactly_the_listed_tools():
    assert set(check.DEFAULT_CALLS) == set(SCOPES)
    for server, calls in check.DEFAULT_CALLS.items():
        assert [tool for tool, _ in calls] == list(SCOPES[server].tools)


# --- source checks --------------------------------------------------------------


@pytest.mark.parametrize("path", SERVER_FILES + TOOL_FILES, ids=lambda p: p.name)
def test_a_server_or_tool_file_never_prints(path):
    """stdout belongs to the protocol."""
    assert not re.search(r"(?m)^\s*print\(", path.read_text(encoding="utf-8"))


@pytest.mark.parametrize("path", SERVER_FILES + TOOL_FILES, ids=lambda p: p.name)
def test_only_the_common_module_touches_databricks_or_the_environment(path):
    text = path.read_text(encoding="utf-8")
    if path.name == "common.py":
        return
    assert not re.search(r"(?m)^\s*(import|from)\s+databricks", text)
    assert "os.environ" not in text and "dotenv" not in text and ".env" not in text
    assert "spark" not in text and ".execute(" not in text


@pytest.mark.parametrize("path", TOOL_FILES, ids=lambda p: p.name)
def test_a_tool_module_holds_no_select_outside_a_template(path):
    """SQL text sits in module constants named *_SQL or in the metric builders."""
    if path.name in ("common.py", "metrics.py"):
        return
    text = path.read_text(encoding="utf-8")
    for match in re.finditer(r"SELECT ", text):
        before = text[: match.start()].splitlines()[-1] if match.start() else ""
        assert "_SQL" in before or before.strip().startswith(('"', "(")), path.name


def test_the_servers_import_only_their_own_tool_modules():
    allowed = {
        "energy_tools": {"energy", "common", "metrics"},
        "commerce_tools": {"commerce", "common", "metrics"},
        "ml_tools": {"ml", "common"},
        "dq_tools": {"dq", "common"},
        "knowledge_tools": {"knowledge"},
        "cross_ecosystem_tools": set(),
    }
    for server, names in allowed.items():
        text = (SERVER_DIR / f"{server}.py").read_text(encoding="utf-8")
        used = set(re.findall(r"from ai\.tools import (\w+)", text))
        assert used == names or used <= names, server
