"""Tool servers -- scope, SQL templates, warehouse access, results, servers.yml.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

The warehouse runs against a fake connection that records the SQL, the parameters
and the connect arguments; nothing reaches Databricks.
"""

from __future__ import annotations

import json
import re
from datetime import date

import pytest
import yaml
from pydantic import ValidationError

from ai.tools import common as cm

pytestmark = [pytest.mark.unit, pytest.mark.ai]

ENERGY_SQL = (
    "SELECT local_date, mean_price_eur_per_mwh "
    "FROM [[energy_analytics.market_price_daily]] "
    "WHERE market_area_code = :area AND local_date >= :start "
    "ORDER BY local_date"
)


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.description = [("local_date",), ("value",)]

    def execute(self, sql, parameters=None):
        self.connection.executed.append((sql, parameters))

    def fetchmany(self, size):
        return self.connection.rows[:size]

    def close(self):
        self.connection.closed_cursors += 1


class FakeConnection:
    def __init__(self, rows):
        self.rows = rows
        self.executed = []
        self.closed_cursors = 0
        self.closed = False

    def cursor(self):
        return FakeCursor(self)

    def close(self):
        self.closed = True


def make_warehouse(rows=()):
    calls = []
    connection = FakeConnection(list(rows))

    def connect(**kwargs):
        calls.append(kwargs)
        return connection

    return cm.Warehouse("host", "/path", "secret", connect=connect), connection, calls


@pytest.fixture(scope="module")
def scopes():
    return cm.load_scopes()


@pytest.fixture
def energy(scopes):
    return scopes["energy_tools"]


def template(sql=ENERGY_SQL, params=("area", "start")):
    return cm.SqlTemplate("market_prices", sql, params)


# --- servers.yml ----------------------------------------------------------------


def test_servers_file_holds_the_six_servers_and_fourteen_tools(scopes):
    assert set(scopes) == {
        "energy_tools",
        "commerce_tools",
        "ml_tools",
        "dq_tools",
        "cross_ecosystem_tools",
        "knowledge_tools",
    }
    names = [tool for scope in scopes.values() for tool in scope.tools]
    assert len(names) == len(set(names)) == 14


def test_every_tool_name_carries_its_domain_prefix(scopes):
    prefix = {
        "energy_tools": "energy_",
        "commerce_tools": "commerce_",
        "ml_tools": "ml_",
        "dq_tools": "dq_",
        "knowledge_tools": "knowledge_",
    }
    for server, scope in scopes.items():
        for tool in scope.tools:
            assert tool.startswith(prefix[server]), tool
            assert re.fullmatch(r"[A-Za-z0-9_]+", tool)


def test_agent_grants_match_the_design_table(scopes):
    grants = {server: set(scope.allowed_agents) for server, scope in scopes.items()}
    assert grants["energy_tools"] == {"energy_specialist"}
    assert grants["commerce_tools"] == {"commerce_specialist"}
    assert grants["ml_tools"] == {"ml_forecasting"}
    assert grants["dq_tools"] == {"dq_trust", "guardrail_check"}
    assert grants["cross_ecosystem_tools"] == {"orchestrator"}
    assert grants["knowledge_tools"] == {
        "orchestrator",
        "energy_specialist",
        "commerce_specialist",
        "ml_forecasting",
        "dq_trust",
    }


def test_the_cross_ecosystem_server_has_no_tool_and_no_schema(scopes):
    scope = scopes["cross_ecosystem_tools"]
    assert scope.tools == () and scope.schemas == frozenset()


def test_no_server_reads_a_raw_layer_or_a_dataset_schema(scopes):
    for scope in scopes.values():
        for schema in scope.schemas:
            assert not cm.is_forbidden_schema(schema), (scope.server, schema)
        for table in scope.extra_tables:
            assert not cm.is_forbidden_schema(table.split(".")[0])


def test_only_the_trust_table_of_quality_is_open_to_the_domain_servers(scopes):
    for server in ("energy_tools", "commerce_tools"):
        scope = scopes[server]
        assert scope.extra_tables == {"quality.quality_audit_log"}
        assert not scope.allows("quality", "quarantine")
        assert scope.allows("quality", "quality_audit_log")


@pytest.mark.parametrize(
    "schema",
    [
        "bronze",
        "energy_silver",
        "commerce_silver_reference",
        "energy_gold",
        "energy_ml_datasets",
        "shared_conformed",
    ],
)
def test_forbidden_schemas_are_recognised(schema):
    assert cm.is_forbidden_schema(schema)


@pytest.mark.parametrize(
    "schema", ["energy_analytics", "commerce_semantic", "ml_models", "quality"]
)
def test_allowed_schemas_are_not_forbidden(schema):
    assert not cm.is_forbidden_schema(schema)


def write_servers(tmp_path, text):
    path = tmp_path / "servers.yml"
    path.write_text(text, encoding="utf-8")
    return path


BASE = """
agents: [orchestrator]
servers:
  one:
    ecosystem: energy
    schemas: [energy_analytics]
    extra_tables: []
    allowed_agents: [orchestrator]
    tools: [one_a, one_b]
"""


def test_a_clean_file_loads(tmp_path):
    assert set(cm.load_scopes(write_servers(tmp_path, BASE))) == {"one"}


@pytest.mark.parametrize(
    "old, new",
    [
        ("[one_a, one_b]", "[one_b, one_a]"),
        ("[one_a, one_b]", "[one_a, bad-name]"),
        ("[energy_analytics]", "[energy_gold]"),
        ("extra_tables: []", "extra_tables: [bronze.x]"),
        ("allowed_agents: [orchestrator]", "allowed_agents: [stranger]"),
    ],
)
def test_a_bad_file_is_refused(tmp_path, old, new):
    with pytest.raises(cm.ScopeError):
        cm.load_scopes(write_servers(tmp_path, BASE.replace(old, new)))


def test_a_tool_name_in_two_servers_is_refused(tmp_path):
    two = BASE + BASE.split("servers:")[1].replace("  one:", "  two:")
    with pytest.raises(cm.ScopeError):
        cm.load_scopes(write_servers(tmp_path, two))


# --- templates ------------------------------------------------------------------


def test_a_template_renders_the_catalog_and_keeps_the_parameters():
    sql = template().render("cat")
    assert "`cat`.`energy_analytics`.`market_price_daily`" in sql
    assert ":area" in sql and "[[" not in sql


def test_the_tables_of_a_template_are_listed():
    assert template().tables() == [("energy_analytics", "market_price_daily")]


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1 FROM bronze.x",
        "SELECT 1 FROM `cat`.`bronze`.`x`",
        "SELECT 1 FROM [[energy_analytics.a]] a JOIN other.b b ON a.k = b.k",
        "SELECT 1 FROM [[energy_analytics.a]]; SELECT 2",
        "SELECT 1 FROM [[energy_analytics.a]] LIMIT 5",
        "SELECT 1 FROM [[energy_analytics.a]] WHERE k = :undeclared",
    ],
)
def test_a_template_that_leaves_the_pattern_is_refused(sql):
    with pytest.raises(cm.ScopeError):
        cm.SqlTemplate("bad", sql, ())


def test_a_declared_parameter_that_the_sql_does_not_use_is_refused():
    with pytest.raises(cm.ScopeError):
        cm.SqlTemplate("bad", "SELECT 1 FROM [[energy_analytics.a]]", ("x",))


def test_a_subquery_is_allowed():
    sql = "SELECT * FROM (SELECT k FROM [[energy_analytics.a]]) t"
    assert cm.SqlTemplate("sub", sql, ()).tables() == [("energy_analytics", "a")]


def test_register_refuses_a_table_outside_the_server(energy):
    other = cm.SqlTemplate("other", "SELECT 1 FROM [[commerce_analytics.x]]", ())
    with pytest.raises(cm.ScopeError):
        energy.register(other)
    quarantine = cm.SqlTemplate("q", "SELECT 1 FROM [[quality.quarantine]]", ())
    with pytest.raises(cm.ScopeError):
        energy.register(quarantine)


def test_register_accepts_the_trust_table_and_the_own_schema(energy):
    trust = cm.SqlTemplate("t", "SELECT 1 FROM [[quality.quality_audit_log]]", ())
    assert energy.register(trust) is trust
    assert energy.register(template()).name == "market_prices"


# --- warehouse ------------------------------------------------------------------


def test_no_connection_opens_before_the_first_query():
    warehouse, _, calls = make_warehouse()
    assert not warehouse.connected and calls == []


def test_the_connection_carries_the_statement_timeout(energy):
    warehouse, _, calls = make_warehouse([(date(2026, 1, 1), 1.0)])
    warehouse.run(energy, template(), {"area": "DE_LU", "start": date(2026, 1, 1)})
    assert calls[0]["session_configuration"] == {"STATEMENT_TIMEOUT": "30"}
    assert calls[0]["server_hostname"] == "host" and warehouse.connected


def test_the_cap_is_in_the_sql_and_rows_are_returned_as_dicts(energy):
    warehouse, connection, _ = make_warehouse([(date(2026, 1, 1), 1.0)])
    result = warehouse.run(
        energy, template(), {"area": "DE_LU", "start": date(2026, 1, 1)}, row_cap=5
    )
    sql, parameters = connection.executed[0]
    assert sql.rstrip().endswith("LIMIT 6") and "`energy_analytics`" in sql
    assert parameters == {"area": "DE_LU", "start": date(2026, 1, 1)}
    assert result.rows == [{"local_date": date(2026, 1, 1), "value": 1.0}]
    assert not result.truncated and connection.closed_cursors == 1


def test_a_longer_result_is_trimmed_and_marked_truncated(energy):
    rows = [(date(2026, 1, 1), float(i)) for i in range(4)]
    warehouse, _, _ = make_warehouse(rows)
    result = warehouse.run(
        energy, template(), {"area": "a", "start": date(2026, 1, 1)}, row_cap=3
    )
    assert len(result.rows) == 3 and result.truncated


@pytest.mark.parametrize("cap", [0, -1, cm.ROW_CAP_MAX + 1])
def test_a_row_cap_outside_the_range_is_refused(energy, cap):
    warehouse, _, _ = make_warehouse()
    with pytest.raises(cm.ScopeError):
        warehouse.run(
            energy, template(), {"area": "a", "start": date(2026, 1, 1)}, row_cap=cap
        )


@pytest.mark.parametrize(
    "params",
    [{}, {"area": "a"}, {"area": "a", "start": date(2026, 1, 1), "x": 1}],
)
def test_parameters_must_match_the_template(energy, params):
    warehouse, _, _ = make_warehouse()
    with pytest.raises(cm.ScopeError):
        warehouse.run(energy, template(), params)


def test_a_parameter_of_a_bad_type_is_refused(energy):
    warehouse, _, _ = make_warehouse()
    with pytest.raises(cm.ScopeError):
        warehouse.run(energy, template(), {"area": ["a"], "start": date(2026, 1, 1)})


def test_a_template_outside_the_scope_is_refused_before_any_connection(energy):
    warehouse, _, calls = make_warehouse()
    bad = cm.SqlTemplate("bad", "SELECT 1 FROM [[commerce_analytics.x]]", ())
    with pytest.raises(cm.ScopeError):
        warehouse.run(energy, bad)
    assert calls == []


def test_close_closes_the_connection_and_the_next_query_opens_again(energy):
    warehouse, connection, calls = make_warehouse([(date(2026, 1, 1), 1.0)])
    args = {"area": "a", "start": date(2026, 1, 1)}
    warehouse.run(energy, template(), args)
    warehouse.close()
    assert connection.closed and not warehouse.connected
    warehouse.run(energy, template(), args)
    assert len(calls) == 2


def test_the_warehouse_from_env_strips_the_scheme_and_names_a_missing_key():
    env = {
        "DATABRICKS_HOST": "https://x.example.com/",
        "DATABRICKS_HTTP_PATH": "/sql/1",
        "DATABRICKS_TOKEN_SQL": "secret",
    }
    assert cm.warehouse_from_env(env)._host == "x.example.com"
    with pytest.raises(RuntimeError, match="DATABRICKS_HTTP_PATH") as info:
        cm.warehouse_from_env({**env, "DATABRICKS_HTTP_PATH": ""})
    assert "secret" not in str(info.value)


# --- results and errors ---------------------------------------------------------


def result(**extra):
    tables = ["energy_analytics.market_price_daily"]
    return {"source_tables": tables, "row_count": 1, **extra}


def test_the_envelope_forbids_extra_fields():
    with pytest.raises(ValidationError):
        cm.ToolResult(**result(surprise=1))


def test_a_trusted_result_needs_a_trust_value():
    with pytest.raises(ValidationError):
        cm.TrustedToolResult(**result())
    ok = cm.TrustedToolResult(**result(trust={"status": "pass"}))
    assert ok.trust.status == "pass" and ok.truncated is False


def test_trust_has_four_states_and_at_most_five_failed_checks():
    with pytest.raises(ValidationError):
        cm.Trust(status="maybe")
    with pytest.raises(ValidationError):
        cm.Trust(status="fail", failed_checks=[str(i) for i in range(6)])


def test_a_tool_failure_carries_a_known_code_and_a_payload():
    failure = cm.ToolFailure("unknown_metric", "no such metric", "list the metrics")
    assert json.loads(json.dumps(failure.payload())) == {
        "code": "unknown_metric",
        "message": "no such metric",
        "hint": "list the metrics",
    }
    with pytest.raises(ValueError):
        cm.ToolFailure("made_up", "x")


def test_the_servers_file_is_valid_yaml_with_the_expected_keys():
    data = yaml.safe_load(cm.SERVERS_FILE.read_text(encoding="utf-8"))
    assert set(data) == {"catalog", "agents", "servers"}
    assert data["catalog"] == cm.CATALOG


def test_the_text_of_a_tool_failure_is_its_json_payload():
    failure = cm.ToolFailure("range_outside_data", "no data", "choose other dates")
    assert json.loads(str(failure)) == failure.payload()


def test_source_text_is_cleaned_and_cut():
    assert cm.clean_text("a\x00b\x1fc") == "a b c"
    assert len(cm.clean_text("x" * 500)) == cm.TEXT_LIMIT
    assert cm.clean_text("abcdef", 3) == "abc"


def test_the_default_range_is_thirty_days_ending_at_the_last_date():
    start, end = cm.default_range(date(2026, 9, 5))
    assert (start, end) == (date(2026, 8, 7), date(2026, 9, 5))
    assert (end - start).days == cm.DEFAULT_DAYS - 1


def test_the_process_warehouse_can_be_replaced_and_cleared():
    warehouse, _, _ = make_warehouse()
    cm.set_warehouse(warehouse)
    assert cm.get_warehouse() is warehouse
    cm.set_warehouse(None)
