"""Shared pieces of the tool servers: scope, SQL templates, warehouse access, results.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

A server reads Databricks only through fixed SQL templates. A template names its
tables as [[schema.table]]; every schema must be in the allow-list of the server
(`ai/mcp_servers/servers.yml`), and the check runs when the template is registered,
at import. The warehouse adds the row cap to the SQL (LIMIT cap + 1, so a longer
result shows as truncated) and every query runs under a statement timeout. The
connection opens on the first query. Library only.
"""

from __future__ import annotations

import json
import os
import re
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field

SERVERS_FILE = Path(__file__).resolve().parents[1] / "mcp_servers" / "servers.yml"
CATALOG = "energy_commerce_retail_media"
ROW_CAP = 200
ROW_CAP_MAX = 1000
QUERY_TIMEOUT_SECONDS = 30
TOKEN_KEY = "DATABRICKS_TOKEN_SQL"
ERROR_CODES = (
    "unknown_metric",
    "wrong_ecosystem",
    "invalid_argument",
    "range_outside_data",
    "limit_exceeded",
    "internal_error",
)
DEFAULT_DAYS = 30
TEXT_LIMIT = 200
Value = str | int | float | bool | date | datetime

_TABLE_REF = re.compile(r"\[\[(\w+)\.(\w+)\]\]")
_RAW_SOURCE = re.compile(r"\b(?:from|join)\s+(?!\[\[|\()", re.IGNORECASE)
_THREE_PART = re.compile(r"\b\w+\.\w+\.\w+\b")
_LIMIT = re.compile(r"\blimit\b", re.IGNORECASE)
_PARAM = re.compile(r"(?<![:\w]):([A-Za-z_]\w*)")
_TOOL_NAME = re.compile(r"^[A-Za-z0-9_]+$")
_CONTROL = re.compile(r"[\x00-\x08\x0b-\x1f\x7f]")
_FORBIDDEN_SCHEMAS = ("bronze", "silver", "gold", "shared_conformed")
_FORBIDDEN_SUFFIXES = ("_silver", "_silver_reference", "_gold", "_ml_datasets")


class ScopeError(Exception):
    """A template, a query or a server list leaves the scope of its server."""


class ToolFailure(Exception):
    """A typed tool error. The SDK turns a raised exception into an error result
    whose text ends with the message, so the message is the JSON payload."""

    def __init__(self, code: str, message: str, hint: str = "") -> None:
        if code not in ERROR_CODES:
            raise ValueError(f"unknown error code {code}")
        self.code = code
        self.message = message
        self.hint = hint
        super().__init__(json.dumps(self.payload(), sort_keys=True))

    def payload(self) -> dict[str, str]:
        return {"code": self.code, "message": self.message, "hint": self.hint}


class Trust(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["pass", "warn", "fail", "unknown"]
    failed_checks: list[str] = Field(default_factory=list, max_length=5)
    checked_at: str | None = None


class ToolResult(BaseModel):
    """The envelope of every tool; a tool adds its own `data` field."""

    model_config = ConfigDict(extra="forbid")

    source_tables: list[str]
    as_of: str | None = None
    row_count: int
    truncated: bool = False
    warnings: list[str] = Field(default_factory=list)


class RowsResult(ToolResult):
    """An envelope whose data is a list of table rows."""

    data: list[dict[str, Any]]


class TrustedToolResult(ToolResult):
    """The envelope of the energy and commerce tools: `trust` is required."""

    trust: Trust


def clean_text(value: object, limit: int = TEXT_LIMIT) -> str:
    """Text from a source: control characters removed, cut to `limit`."""
    return _CONTROL.sub(" ", str(value))[:limit]


def json_value(value: Any) -> Any:
    """A date or time as ISO text; other values unchanged."""
    return value.isoformat() if isinstance(value, (date, datetime)) else value


def default_range(as_of: date, days: int = DEFAULT_DAYS) -> tuple[date, date]:
    """The last `days` days up to `as_of`; the sources are static, not today."""
    return as_of - timedelta(days=days - 1), as_of


def is_forbidden_schema(schema: str) -> bool:
    """A raw layer, the ML datasets or the shared dimensions: no tool reads them."""
    return schema in _FORBIDDEN_SCHEMAS or schema.endswith(_FORBIDDEN_SUFFIXES)


@dataclass(frozen=True)
class SqlTemplate:
    """Fixed SQL. Tables are [[schema.table]], parameters are :name."""

    name: str
    sql: str
    params: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if ";" in self.sql:
            raise ScopeError(f"{self.name}: a template holds one statement, no ';'")
        if _LIMIT.search(self.sql):
            raise ScopeError(f"{self.name}: the warehouse adds the row cap, no LIMIT")
        if _RAW_SOURCE.search(self.sql):
            raise ScopeError(f"{self.name}: FROM and JOIN take [[schema.table]]")
        if _THREE_PART.search(_TABLE_REF.sub("", self.sql)):
            raise ScopeError(f"{self.name}: no catalog.schema.table names")
        if set(_PARAM.findall(self.sql)) != set(self.params):
            raise ScopeError(f"{self.name}: :names in the SQL differ from params")

    def tables(self) -> list[tuple[str, str]]:
        return [(m.group(1), m.group(2)) for m in _TABLE_REF.finditer(self.sql)]

    def render(self, catalog: str = CATALOG) -> str:
        return _TABLE_REF.sub(
            lambda m: f"`{catalog}`.`{m.group(1)}`.`{m.group(2)}`", self.sql
        )


@dataclass(frozen=True)
class ServerScope:
    server: str
    ecosystem: str
    schemas: frozenset[str]
    extra_tables: frozenset[str]
    allowed_agents: tuple[str, ...]
    tools: tuple[str, ...]

    def allows(self, schema: str, table: str) -> bool:
        return schema in self.schemas or f"{schema}.{table}" in self.extra_tables

    def register(self, template: SqlTemplate) -> SqlTemplate:
        """Check the tables of a template against this server; returns it."""
        for schema, table in template.tables():
            if not self.allows(schema, table):
                raise ScopeError(
                    f"{self.server}: {template.name} reads {schema}.{table}, "
                    "which is not in the scope of the server"
                )
        return template


def load_scopes(path: Path = SERVERS_FILE) -> dict[str, ServerScope]:
    """The scope of every server; a malformed list raises ScopeError."""
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    agents = set(data["agents"])
    scopes: dict[str, ServerScope] = {}
    seen: dict[str, str] = {}
    for server, item in data["servers"].items():
        tools = tuple(item["tools"])
        if list(tools) != sorted(tools):
            raise ScopeError(f"{server}: tools are not sorted by name")
        for tool in tools:
            if not _TOOL_NAME.match(tool):
                raise ScopeError(f"{server}: bad tool name {tool}")
            if tool in seen:
                raise ScopeError(f"tool {tool} is in {seen[tool]} and {server}")
            seen[tool] = server
        for schema in item["schemas"]:
            if is_forbidden_schema(schema):
                raise ScopeError(f"{server}: schema {schema} is forbidden")
        for table in item["extra_tables"]:
            if is_forbidden_schema(table.split(".")[0]):
                raise ScopeError(f"{server}: table {table} is forbidden")
        unknown = set(item["allowed_agents"]) - agents
        if unknown:
            raise ScopeError(f"{server}: unknown agents {sorted(unknown)}")
        scopes[server] = ServerScope(
            server=server,
            ecosystem=item["ecosystem"],
            schemas=frozenset(item["schemas"]),
            extra_tables=frozenset(item["extra_tables"]),
            allowed_agents=tuple(item["allowed_agents"]),
            tools=tools,
        )
    return scopes


@dataclass(frozen=True)
class QueryResult:
    rows: list[dict[str, Any]]
    truncated: bool
    elapsed_ms: int


class Warehouse:
    """Runs registered templates on a Databricks SQL warehouse."""

    def __init__(
        self,
        host: str,
        http_path: str,
        token: str,
        *,
        catalog: str = CATALOG,
        timeout_seconds: int = QUERY_TIMEOUT_SECONDS,
        connect: Callable[..., Any] | None = None,
    ) -> None:
        self._host = host
        self._http_path = http_path
        self._token = token
        self._catalog = catalog
        self._timeout = int(timeout_seconds)
        self._connect = connect
        self._connection: Any = None

    @property
    def connected(self) -> bool:
        return self._connection is not None

    def _open(self) -> Any:
        if self._connection is None:
            connect = self._connect
            if connect is None:
                from databricks import sql

                connect = sql.connect
            self._connection = connect(
                server_hostname=self._host,
                http_path=self._http_path,
                access_token=self._token,
                session_configuration={"STATEMENT_TIMEOUT": str(self._timeout)},
            )
        return self._connection

    def run(
        self,
        scope: ServerScope,
        template: SqlTemplate,
        params: Mapping[str, Value] | None = None,
        *,
        row_cap: int = ROW_CAP,
    ) -> QueryResult:
        scope.register(template)
        if not 1 <= row_cap <= ROW_CAP_MAX:
            raise ScopeError(f"row cap {row_cap} is outside 1 to {ROW_CAP_MAX}")
        given = dict(params or {})
        if set(given) != set(template.params):
            raise ScopeError(f"{template.name}: parameters differ from the template")
        for key, value in given.items():
            if not isinstance(value, Value):
                raise ScopeError(f"{template.name}: parameter {key} has a bad type")
        sql = f"{template.render(self._catalog)}\nLIMIT {row_cap + 1}"
        started = time.monotonic()
        cursor = self._open().cursor()
        try:
            cursor.execute(sql, given or None)
            names = [column[0] for column in cursor.description]
            fetched = cursor.fetchmany(row_cap + 1)
        finally:
            cursor.close()
        rows = [dict(zip(names, row, strict=True)) for row in fetched]
        return QueryResult(
            rows=rows[:row_cap],
            truncated=len(rows) > row_cap,
            elapsed_ms=int((time.monotonic() - started) * 1000),
        )

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None


def warehouse_from_env(env: Mapping[str, str], token_key: str = TOKEN_KEY) -> Warehouse:
    """A warehouse from a mapping of environment values; a missing key is named."""
    for key in ("DATABRICKS_HOST", "DATABRICKS_HTTP_PATH", token_key):
        if not env.get(key):
            raise RuntimeError(f"the environment has no {key}")
    host = env["DATABRICKS_HOST"].removeprefix("https://").rstrip("/")
    return Warehouse(
        host,
        env["DATABRICKS_HTTP_PATH"],
        env[token_key],
        catalog=env.get("DATABRICKS_CATALOG") or CATALOG,
    )


_WAREHOUSE: Warehouse | None = None


def get_warehouse() -> Warehouse:
    """The warehouse of this server process, made from the environment on first use."""
    global _WAREHOUSE
    if _WAREHOUSE is None:
        _WAREHOUSE = warehouse_from_env(os.environ)
    return _WAREHOUSE


def set_warehouse(warehouse: Warehouse | None) -> None:
    """Replace the warehouse of the process; the tests use it."""
    global _WAREHOUSE
    _WAREHOUSE = warehouse
