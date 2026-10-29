"""Boundary check: each server's token reads its own tables and is refused on the rest.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

For each of the four servers that read Databricks, the check connects with the token
of that server (from `.env`), reads one row of each table it may read, then tries one
row of tables in schemas it must not read. A read of an allowed table must work; a
read of any other table must be refused by Databricks. The matrix goes to
`src/findings/tools_findings/boundary.md`. The tables are listed here and a test
compares the list with `servers.yml`.

Cost: about 11 small queries for each of four principals, each cut off at 30 s. The
first failed allowed read stops that principal. Worst case about four minutes.

Usage (repository root):
    python3 -m ai.mcp_servers.boundary
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from ai.mcp_servers import credentials
from ai.tools import common as cm
from databricks import sql

FINDINGS = (
    Path(__file__).resolve().parents[2] / "src/findings/tools_findings/boundary.md"
)
TIMEOUT = 30
DENIED = (
    "insufficient_permissions",
    "permission_denied",
    "does not have",
    "not authorized",
    "permission denied",
)
OTHER_ECOSYSTEM_AND_LAYERS = (
    ("bronze", "dwd_air_temperature"),
    ("ml_datasets_shared", "dataset_manifest"),
    ("shared_conformed", "dim_date"),
)
CHECKS = {
    "energy_tools": {
        "allowed": (
            ("energy_analytics", "market_price_daily"),
            ("energy_semantic", "metric_definition"),
            ("quality", "quality_audit_log"),
        ),
        "refused": (
            ("quality", "quarantine"),
            ("energy_silver", "electricity_balance"),
            ("energy_gold", "capacity_plan"),
            ("commerce_analytics", "ga4_demand_daily"),
            ("ml_models", "model_registry"),
            *OTHER_ECOSYSTEM_AND_LAYERS,
        ),
    },
    "commerce_tools": {
        "allowed": (
            ("commerce_analytics", "ga4_demand_daily"),
            ("commerce_semantic", "metric_definition"),
            ("quality", "quality_audit_log"),
        ),
        "refused": (
            ("quality", "quarantine"),
            ("commerce_silver", "ga4_event"),
            ("commerce_gold", "ga4_session"),
            ("energy_analytics", "market_price_daily"),
            ("ml_models", "model_registry"),
            *OTHER_ECOSYSTEM_AND_LAYERS,
        ),
    },
    "ml_tools": {
        "allowed": (
            ("ml_models", "model_registry"),
            ("ml_models", "monitoring_results"),
            ("ml_models", "model_predictions"),
        ),
        "refused": (
            ("quality", "quality_audit_log"),
            ("energy_analytics", "market_price_daily"),
            ("commerce_analytics", "ga4_demand_daily"),
            ("energy_gold", "capacity_plan"),
            ("energy_ml_datasets", "dataset_bias"),
            *OTHER_ECOSYSTEM_AND_LAYERS,
        ),
    },
    "dq_tools": {
        "allowed": (
            ("quality", "quality_audit_log"),
            ("quality", "quarantine"),
            ("quality", "pipeline_watermarks"),
            ("quality", "field_class_registry"),
        ),
        "refused": (
            ("ml_models", "model_registry"),
            ("energy_analytics", "market_price_daily"),
            ("commerce_analytics", "ga4_demand_daily"),
            ("energy_gold", "capacity_plan"),
            *OTHER_ECOSYSTEM_AND_LAYERS,
        ),
    },
}


@dataclass
class Row:
    server: str
    table: str
    expected: str
    result: str
    note: str = ""

    @property
    def good(self) -> bool:
        return self.result == self.expected

    def text(self) -> str:
        mark = "ok  " if self.good else "FAIL"
        return f"{mark} {self.server:15} {self.expected:8} {self.table:50} {self.note}"


def read_one(connection, catalog: str, schema: str, table: str) -> str | None:
    """None when one row was read; the error text when the read failed."""
    cursor = connection.cursor()
    try:
        cursor.execute(f"SELECT * FROM `{catalog}`.`{schema}`.`{table}` LIMIT 1")
        cursor.fetchall()
    except Exception as exc:
        return f"{type(exc).__name__}: {exc}"
    finally:
        cursor.close()
    return None


def classify(error: str | None) -> str:
    if error is None:
        return "read"
    if any(word in error.lower() for word in DENIED):
        return "refused"
    return "error"


def check_server(server: str, values: dict[str, str]) -> list[Row]:
    key = cm.TOKEN_KEYS[server]
    token = values.get(key)
    if not token:
        return [Row(server, "-", "read", "error", f".env has no {key}")]
    catalog = values.get("DATABRICKS_CATALOG") or cm.CATALOG
    connection = sql.connect(
        server_hostname=values["DATABRICKS_HOST"].removeprefix("https://").rstrip("/"),
        http_path=values["DATABRICKS_HTTP_PATH"],
        access_token=token,
        session_configuration={"STATEMENT_TIMEOUT": str(TIMEOUT)},
    )
    rows: list[Row] = []
    try:
        for schema, table in CHECKS[server]["allowed"]:
            error = read_one(connection, catalog, schema, table)
            result = classify(error)
            note = (
                "" if error is None else cm.clean_text(cm.redact(error, [token]), 120)
            )
            rows.append(Row(server, f"{schema}.{table}", "read", result, note))
            print(rows[-1].text(), flush=True)
            if result != "read":
                return rows
        for schema, table in CHECKS[server]["refused"]:
            error = read_one(connection, catalog, schema, table)
            result = classify(error)
            note = "" if result == "refused" else cm.redact(str(error), [token])[:120]
            rows.append(Row(server, f"{schema}.{table}", "refused", result, note))
            print(rows[-1].text(), flush=True)
    finally:
        connection.close()
    return rows


def write_findings(rows: list[Row]) -> None:
    FINDINGS.parent.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
    body = "\n".join(
        f"| {r.server} | {r.table} | {r.expected} | {r.result} | {r.note} |"
        for r in rows
    )
    FINDINGS.write_text(
        f"# Tool servers: boundary check\n\nRun: {stamp}\n\n"
        "Each server reads with its own token. `read` must work, `refused` must be "
        "refused by Databricks.\n\n"
        "| server | table | expected | result | note |\n|---|---|---|---|---|\n"
        f"{body}\n",
        encoding="utf-8",
    )


def main() -> int:
    values = credentials.load_values()
    missing = [
        k for k in ("DATABRICKS_HOST", "DATABRICKS_HTTP_PATH") if not values.get(k)
    ]
    if missing:
        print(f"STOP: .env has no {', '.join(missing)}")
        return 1
    rows: list[Row] = []
    for server in CHECKS:
        print(f"== {server}", flush=True)
        try:
            rows += check_server(server, values)
        except Exception as exc:
            rows.append(Row(server, "-", "read", "error", type(exc).__name__))
            print(rows[-1].text(), flush=True)
    write_findings(rows)
    bad = [r for r in rows if not r.good]
    print(
        f"{len(rows) - len(bad)} of {len(rows)} lines as expected; written {FINDINGS}"
    )
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
