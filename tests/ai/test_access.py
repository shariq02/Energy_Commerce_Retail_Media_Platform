"""Access to the tool servers: grants, the environment of a server, redaction, tokens.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

A fake secret is built at run time, so no token-like text sits in this file. The
subprocess test starts a server for an agent that has no grant; it needs no
Databricks and no token.
"""

from __future__ import annotations

import ast
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

from ai.mcp_servers import boundary, check, credentials, grants
from ai.tools import common as cm

pytestmark = [pytest.mark.unit, pytest.mark.ai]

ROOT = Path(__file__).resolve().parents[2]
SCOPES = cm.load_scopes()
FAKE = "dapi" + "0123456789abcdef" * 2
ENERGY, COMMERCE, ML, DQ = (
    "ECRMAP_TOOLS_ENERGY_TOKEN",
    "ECRMAP_TOOLS_COMMERCE_TOKEN",
    "ECRMAP_TOOLS_ML_TOKEN",
    "ECRMAP_TOOLS_DQ_TOKEN",
)
VALUES = {
    "DATABRICKS_HOST": "https://x.example.com",
    "DATABRICKS_HTTP_PATH": "/sql/1",
    "DATABRICKS_CATALOG": "cat",
    ENERGY: "energy-secret-value",
    COMMERCE: "commerce-secret-value",
    ML: "ml-secret-value",
    DQ: "dq-secret-value",
    "DATABRICKS_TOKEN_SQL": "shared-sql-secret",
    "DATABRICKS_TOKEN_ADMIN": "admin-secret-value",
    "OTHER_SECRET": "unrelated-secret",
}


@pytest.fixture(autouse=True)
def reset_warehouse():
    cm.set_warehouse(None)
    yield
    cm.set_warehouse(None)


# --- grants ---------------------------------------------------------------------

AGENTS = sorted({a for scope in SCOPES.values() for a in scope.allowed_agents})
PAIRS = [(agent, server) for server in sorted(SCOPES) for agent in AGENTS]


@pytest.mark.parametrize("agent, server", PAIRS)
def test_a_grant_is_exactly_the_list_of_servers_yml(agent, server):
    wanted = agent in SCOPES[server].allowed_agents
    assert grants.is_allowed(agent, server) is wanted
    if wanted:
        grants.require(agent, server)
    else:
        with pytest.raises(grants.GrantDenied):
            grants.require(agent, server)


def test_an_unknown_server_or_agent_has_no_grant():
    assert not grants.is_allowed("orchestrator", "no_such_server")
    assert not grants.is_allowed("", "knowledge_tools")
    assert not grants.is_allowed("a_model_made_up_name", "knowledge_tools")


def test_only_the_orchestrator_may_start_the_cross_ecosystem_server():
    assert SCOPES["cross_ecosystem_tools"].allowed_agents == ("orchestrator",)
    assert set(SCOPES["dq_tools"].allowed_agents) == {"dq_trust", "guardrail_check"}


def test_the_start_check_lets_a_granted_agent_through():
    grants.check_start("energy_tools", {cm.AGENT_KEY: "energy_specialist"})


@pytest.mark.parametrize("env", [{cm.AGENT_KEY: "commerce_specialist"}, {}])
def test_the_start_check_stops_a_denied_or_unnamed_agent(env, capsys):
    with pytest.raises(SystemExit) as info:
        grants.check_start("energy_tools", env)
    assert (
        info.value.code == 2 and "may not start energy_tools" in capsys.readouterr().err
    )


def test_a_server_started_for_an_agent_without_a_grant_exits_with_code_two():
    env = {k: v for k, v in os.environ.items() if not k.startswith("ECRMAP_")}
    env.update({"PYTHONPATH": str(ROOT), cm.AGENT_KEY: "energy_specialist"})
    done = subprocess.run(
        [sys.executable, "-m", "ai.mcp_servers.cross_ecosystem_tools"],
        env=env,
        capture_output=True,
        text=True,
        stdin=subprocess.DEVNULL,
        timeout=60,
        check=False,
    )
    assert done.returncode == 2 and done.stdout == ""
    assert "may not start cross_ecosystem_tools" in done.stderr


@pytest.mark.parametrize("server", sorted(SCOPES))
def test_every_server_asks_for_its_grant_before_it_runs(server):
    text = (ROOT / "ai" / "mcp_servers" / f"{server}.py").read_text(encoding="utf-8")
    start = text.index('if __name__ == "__main__":')
    block = text[start:]
    assert block.index(f'grants.check_start("{server}")') < block.index("app.run")


# --- the environment of a server ------------------------------------------------


def test_a_server_gets_its_own_token_and_no_other_secret():
    env = credentials.spawn_env("energy_specialist", "energy_tools", VALUES)
    assert env[ENERGY] == VALUES[ENERGY]
    assert env[cm.SERVER_KEY] == "energy_tools"
    assert env[cm.AGENT_KEY] == "energy_specialist"
    assert env["DATABRICKS_HOST"] == VALUES["DATABRICKS_HOST"]
    assert env["DATABRICKS_CATALOG"] == "cat" and "PYTHONPATH" in env
    for secret in (COMMERCE, ML, DQ, "DATABRICKS_TOKEN_SQL", "DATABRICKS_TOKEN_ADMIN"):
        assert secret not in env
    assert "OTHER_SECRET" not in env
    assert not any(v in env.values() for v in (VALUES[COMMERCE], VALUES[ML]))


@pytest.mark.parametrize("server, key", sorted(cm.TOKEN_KEYS.items()))
def test_each_databricks_server_gets_only_its_key(server, key):
    agent = SCOPES[server].allowed_agents[0]
    env = credentials.spawn_env(agent, server, VALUES)
    assert [k for k in env if k.startswith("ECRMAP_TOOLS_")] == [key]


def test_a_server_without_databricks_gets_no_databricks_key():
    env = credentials.spawn_env("orchestrator", "knowledge_tools", VALUES)
    assert set(env) == {"PYTHONPATH", cm.SERVER_KEY, cm.AGENT_KEY}
    env = credentials.spawn_env("orchestrator", "cross_ecosystem_tools", VALUES)
    assert set(env) == {"PYTHONPATH", cm.SERVER_KEY, cm.AGENT_KEY}


def test_no_environment_is_made_for_an_agent_without_a_grant():
    with pytest.raises(grants.GrantDenied):
        credentials.spawn_env("energy_specialist", "ml_tools", VALUES)


def test_a_missing_key_is_named_and_no_value_is_shown():
    values = {k: v for k, v in VALUES.items() if k != ML}
    with pytest.raises(credentials.MissingCredential) as info:
        credentials.spawn_env("ml_forecasting", "ml_tools", values)
    text = str(info.value)
    assert ML in text and not any(v in text for v in VALUES.values())


def test_the_env_file_is_read_at_every_call_and_empty_values_are_dropped(tmp_path):
    path = tmp_path / ".env"
    path.write_text(f"A=1\nB=\n{ENERGY}=first\n", encoding="utf-8")
    assert credentials.load_values(path) == {"A": "1", ENERGY: "first"}
    path.write_text(f"{ENERGY}=second\n", encoding="utf-8")
    assert credentials.load_values(path) == {ENERGY: "second"}


def test_the_smoke_check_starts_each_server_for_its_first_granted_agent():
    env = check.server_env("dq_tools", VALUES)
    assert env[cm.AGENT_KEY] == SCOPES["dq_tools"].allowed_agents[0]
    assert env[DQ] == VALUES[DQ]


# --- the warehouse of a server --------------------------------------------------


def server_environment(monkeypatch, server, **extra):
    for key in (cm.SERVER_KEY, *cm.TOKEN_KEYS.values(), cm.TOKEN_KEY):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("DATABRICKS_HOST", "x.example.com")
    monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/1")
    if server:
        monkeypatch.setenv(cm.SERVER_KEY, server)
    for key, value in extra.items():
        monkeypatch.setenv(key, value)


def test_a_named_server_uses_its_own_token(monkeypatch):
    server_environment(
        monkeypatch, "energy_tools", **{ENERGY: "own-token", cm.TOKEN_KEY: "shared"}
    )
    assert cm.get_warehouse()._token == "own-token"


def test_a_named_server_without_its_key_fails_and_does_not_fall_back(monkeypatch):
    server_environment(monkeypatch, "energy_tools", **{cm.TOKEN_KEY: "shared"})
    with pytest.raises(RuntimeError, match=ENERGY) as info:
        cm.get_warehouse()
    assert "shared" not in str(info.value)


def test_without_a_server_name_the_shared_key_is_used(monkeypatch):
    server_environment(monkeypatch, None, **{cm.TOKEN_KEY: "shared"})
    assert cm.get_warehouse()._token == "shared"


# --- redaction and typed failures -----------------------------------------------


def test_redact_removes_token_patterns_and_given_secrets():
    text = f"failed with {FAKE} and Bearer abcdefgh12345678 and my-own-secret-1"
    out = cm.redact(text, ["my-own-secret-1"])
    assert FAKE not in out and "abcdefgh12345678" not in out
    assert "my-own-secret-1" not in out and out.count("***") == 3


def test_redact_removes_the_secrets_of_the_environment(monkeypatch):
    monkeypatch.setenv(ENERGY, "environment-secret")
    assert "environment-secret" not in cm.redact("x environment-secret y")


def test_redact_leaves_short_values_and_plain_text(monkeypatch):
    monkeypatch.setenv(ENERGY, "abc")
    assert cm.redact("abc and plain words") == "abc and plain words"


def test_a_guarded_tool_turns_an_unexpected_error_into_internal_error(capsys):
    @cm.guarded
    def tool():
        raise RuntimeError(f"connection failed for {FAKE}")

    with pytest.raises(cm.ToolFailure) as info:
        tool()
    assert info.value.code == "internal_error" and FAKE not in str(info.value)
    err = capsys.readouterr().err
    assert "tool tool failed" in err and FAKE not in err and "RuntimeError" in err


def test_a_guarded_tool_keeps_a_typed_failure_and_a_result():
    @cm.guarded
    def refuse():
        raise cm.ToolFailure("unknown_metric", "no such metric")

    @cm.guarded
    def work(a, b=2):
        return a + b

    with pytest.raises(cm.ToolFailure) as info:
        refuse()
    assert info.value.code == "unknown_metric"
    assert work(1, b=3) == 4 and work.__name__ == "work"


# --- the list of the boundary check and the setup notebook ----------------------


def test_the_boundary_check_covers_the_four_databricks_servers():
    assert set(boundary.CHECKS) == set(cm.TOKEN_KEYS)


@pytest.mark.parametrize("server", sorted(cm.TOKEN_KEYS))
def test_the_boundary_lists_follow_the_scope_of_servers_yml(server):
    scope = SCOPES[server]
    for schema, table in boundary.CHECKS[server]["allowed"]:
        assert scope.allows(schema, table), (server, schema, table)
    for schema, table in boundary.CHECKS[server]["refused"]:
        assert not scope.allows(schema, table), (server, schema, table)


def test_the_setup_notebook_grants_the_scope_of_servers_yml():
    tree = ast.parse(
        (ROOT / "databricks" / "setup" / "05_tool_principals.py").read_text("utf-8")
    )
    grants_node = next(
        n.value
        for n in tree.body
        if isinstance(n, ast.Assign) and n.targets[0].id == "GRANTS"
    )
    notebook = ast.literal_eval(grants_node)
    assert set(notebook) == {"energy", "commerce", "ml", "dq"}
    for name, item in notebook.items():
        scope = SCOPES[f"{name}_tools"]
        assert set(item["schemas"]) == set(scope.schemas), name
        assert set(item["tables"]) == set(scope.extra_tables), name


# --- secrets in files -----------------------------------------------------------


def test_the_example_file_names_the_four_keys_without_values():
    lines = (ROOT / ".env.example").read_text(encoding="utf-8").splitlines()
    for key in cm.TOKEN_KEYS.values():
        assert f"{key}=" in lines


def test_only_the_credentials_module_reads_the_env_file():
    for folder in ("ai/mcp_servers", "ai/tools"):
        for path in sorted((ROOT / folder).glob("*.py")):
            text = path.read_text(encoding="utf-8")
            if path.name == "credentials.py":
                continue
            assert "dotenv" not in text, path.name


def test_no_file_of_the_code_holds_a_token():
    pattern = re.compile(r"dapi[a-f0-9]{20,}")
    suffixes = {".py", ".md", ".yml", ".json", ".sql", ".txt"}
    for folder in ("ai", "tests", "scripts", "databricks", "src"):
        for path in (ROOT / folder).rglob("*"):
            if path.suffix not in suffixes or "vector_store" in path.parts:
                continue
            small = path.is_file() and path.stat().st_size < 2_000_000
            if small and pattern.search(path.read_text("utf-8", "ignore")):
                pytest.fail(f"a token pattern is in {path.relative_to(ROOT)}")
