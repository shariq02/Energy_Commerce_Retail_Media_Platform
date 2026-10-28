"""Smoke check of the tool servers: start each one on stdio, list and call its tools.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

For each server: start it as a subprocess, compare `tools/list` with `servers.yml`,
call each tool once with a small default request and print one line per call (status,
rows, time). A server that fails a call is not called again. After the servers it
compares `metrics.yml` with the live `metric_definition` rows and writes the table to
`src/findings/tools_findings/smoke_check.md`.

Cost of a healthy run: 14 tool calls, about 25 small queries on the SQL warehouse
(each cut off at 30 s), one model load for the knowledge server. Limits: 60 s to start
a server, 90 s per call; the worst case is 6 servers times 150 s, because a server is
dropped after its first failure. The Databricks keys of `.env` go only to the four
servers that read Databricks.

Usage (repository root):
    python3 -m ai.mcp_servers.check
"""

from __future__ import annotations

import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import anyio
from dotenv import dotenv_values
from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client

from ai.tools import commerce, energy
from ai.tools import common as cm

REPO_ROOT = Path(__file__).resolve().parents[2]
FINDINGS = REPO_ROOT / "src" / "findings" / "tools_findings" / "smoke_check.md"
START_SECONDS = 60
CALL_SECONDS = 90
DATABRICKS_KEYS = (
    "DATABRICKS_HOST",
    "DATABRICKS_HTTP_PATH",
    "DATABRICKS_TOKEN_SQL",
    "DATABRICKS_CATALOG",
)
USES_DATABRICKS = ("energy_tools", "commerce_tools", "ml_tools", "dq_tools")
DEFAULT_CALLS: dict[str, list[tuple[str, dict]]] = {
    "energy_tools": [
        (
            "energy_analyse_market_conditions",
            {"market_area_code": "de_lu"},
        ),
        ("energy_analyse_weather_conditions", {"location_key": "*"}),
        ("energy_compare_periods", {"metric_key": "average_market_price"}),
        (
            "energy_get_metric",
            {
                "metric_key": "average_market_price",
                "filters": {"market_area_code": "de_lu"},
                "limit": 10,
            },
        ),
    ],
    "commerce_tools": [
        ("commerce_compare_periods", {"metric_key": "ga4_sessions"}),
        ("commerce_get_metric", {"metric_key": "ga4_sessions", "limit": 10}),
    ],
    "ml_tools": [
        ("ml_read_model_card", {"task_id": "price_daily.price"}),
        (
            "ml_read_monitoring",
            {"task_id": "lapse_ga4.return", "kind": "results", "limit": 5},
        ),
        (
            "ml_read_predictions",
            {"task_id": "session_purchase_rees46.purchase", "limit": 5},
        ),
    ],
    "dq_tools": [
        ("dq_read_field_class", {"limit": 5}),
        ("dq_read_quality_log", {"source": "smard", "limit": 5}),
        ("dq_read_quarantine", {"source_system": "dwd", "limit": 3}),
        ("dq_read_watermarks", {"source": "smard", "limit": 5}),
    ],
    "cross_ecosystem_tools": [],
    "knowledge_tools": [
        (
            "knowledge_retrieve",
            {"query": "what is the grain of the average market price metric", "k": 3},
        ),
    ],
}


@dataclass
class Line:
    server: str
    tool: str
    status: str
    rows: str
    seconds: float
    note: str = ""

    def text(self) -> str:
        return (
            f"{self.status:5} {self.server:22} {self.tool:34} rows {self.rows:>5} "
            f"{self.seconds:7.1f} s  {self.note}"
        )


def server_env(server: str, values: dict[str, str]) -> dict[str, str]:
    env = {"PYTHONPATH": str(REPO_ROOT)}
    if server in USES_DATABRICKS:
        env.update({k: values[k] for k in DATABRICKS_KEYS if values.get(k)})
    return env


def describe(result) -> tuple[str, str, str]:
    """(status, rows, note) of one call result."""
    if result.isError:
        text = next((c.text for c in result.content if hasattr(c, "text")), "")
        return "FAIL", "-", cm.clean_text(text, 160)
    data = result.structuredContent or {}
    notes = data.get("warnings") or []
    note = f"as_of {data.get('as_of')}"
    if notes:
        note += f"; {len(notes)} warning(s)"
    return "ok", str(data.get("row_count", "-")), note


async def call_all(
    session: ClientSession, server: str, expected: list[str], began: float
) -> list[Line]:
    listed = [tool.name for tool in (await session.list_tools()).tools]
    same = listed == expected
    lines = [
        Line(
            server,
            "(tools/list)",
            "ok" if same else "FAIL",
            str(len(listed)),
            time.monotonic() - began,
            "start and list" if same else f"got {listed}",
        )
    ]
    failed = not same
    for tool, args in DEFAULT_CALLS[server]:
        if failed:
            lines.append(Line(server, tool, "skip", "-", 0.0, "after a failure"))
            continue
        started = time.monotonic()
        try:
            with anyio.fail_after(CALL_SECONDS + 5):
                result = await session.call_tool(
                    tool, args, read_timeout_seconds=timedelta(seconds=CALL_SECONDS)
                )
            status, rows, note = describe(result)
        except Exception as exc:
            status, rows, note = "FAIL", "-", type(exc).__name__
        failed = status != "ok"
        lines.append(Line(server, tool, status, rows, time.monotonic() - started, note))
        print(lines[-1].text(), flush=True)
    return lines


async def check_server(server: str, values: dict[str, str]) -> list[Line]:
    params = StdioServerParameters(
        command=sys.executable,
        args=["-m", f"ai.mcp_servers.{server}"],
        env=server_env(server, values),
        cwd=REPO_ROOT,
    )
    expected = list(cm.load_scopes()[server].tools)
    began = time.monotonic()
    async with (
        stdio_client(params) as (read, write),
        ClientSession(read, write) as session,
    ):
        with anyio.fail_after(START_SECONDS):
            await session.initialize()
        return await call_all(session, server, expected, began)


def bindings(values: dict[str, str]) -> list[str]:
    cm.set_warehouse(cm.warehouse_from_env(values))
    return [
        *[f"energy {p}" for p in energy.DOMAIN.verify_bindings()],
        *[f"commerce {p}" for p in commerce.DOMAIN.verify_bindings()],
    ]


def write_findings(lines: list[Line], problems: list[str]) -> None:
    FINDINGS.parent.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
    rows = "\n".join(
        f"| {x.server} | {x.tool} | {x.status} | {x.rows} | {x.seconds:.1f} "
        f"| {x.note} |"
        for x in lines
    )
    verdict = "\n".join(f"- {p}" for p in problems) or "- none: keys and marts match"
    FINDINGS.write_text(
        f"# Tool servers: smoke check\n\nRun: {stamp}\n\n"
        "| server | tool | status | rows | seconds | note |\n"
        "|---|---|---|---|---|---|\n"
        f"{rows}\n\n## metrics.yml against metric_definition\n\n{verdict}\n",
        encoding="utf-8",
    )


async def main_async() -> int:
    values = {k: v for k, v in dotenv_values(REPO_ROOT / ".env").items() if v}
    lines: list[Line] = []
    for server in cm.load_scopes():
        print(f"== {server}", flush=True)
        try:
            lines += await check_server(server, values)
        except Exception as exc:
            lines.append(Line(server, "(start)", "FAIL", "-", 0.0, type(exc).__name__))
            print(lines[-1].text(), flush=True)
    print("== metrics.yml against metric_definition", flush=True)
    try:
        problems = bindings(values)
    except Exception as exc:
        problems = [f"the check failed: {type(exc).__name__}"]
    for problem in problems:
        print("   ", problem)
    write_findings(lines, problems)
    bad = [x for x in lines if x.status == "FAIL"]
    print(f"{len(lines) - len(bad)} of {len(lines)} lines ok; written {FINDINGS}")
    return 1 if bad or problems else 0


def main() -> int:
    return anyio.run(main_async)


if __name__ == "__main__":
    sys.exit(main())
