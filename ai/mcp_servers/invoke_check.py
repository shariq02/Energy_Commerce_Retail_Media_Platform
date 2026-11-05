"""Check of the invocation layer: every tool through `invoke_tool`, plus wrong calls.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

For each server the command opens one run context for the first agent that has a
grant, calls every tool once with the small default request of the smoke check (the
knowledge server repeats its first call, to show the time after the models are
loaded), then makes two deliberate wrong calls: the first tool with an argument
that does not exist (expected `invalid_arguments`) and, when an agent without a
grant exists, a call by that agent (expected `denied`, no server starts). It prints
one line per call (status, time, retries, trust, the untrusted fields found) and
writes the table to `src/findings/tools_findings/invocation.md`.

Cost of a healthy run: the 14 calls of the smoke check plus up to 10 wrong calls,
which cost no query. Limits: 60 s to start a server, 60 s per call, three attempts
at most per call; the layer retries only a transient failure.
The log lines of the run go to `logs/tool_calls/` (not tracked).

Usage (repository root):
    python3 -m ai.mcp_servers.invoke_check
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import anyio

from ai.mcp_servers import call_log, check, credentials, invoke
from ai.tools import common as cm

REPO_ROOT = Path(__file__).resolve().parents[2]
FINDINGS = REPO_ROOT / "src" / "findings" / "tools_findings" / "invocation.md"


REPEAT_FIRST_CALL = ("knowledge_tools",)


@dataclass
class Row:
    server: str
    tool: str
    expected: str
    status: str
    millis: int
    retries: int
    trust: str
    untrusted: str
    note: str

    @property
    def good(self) -> bool:
        return self.status == self.expected

    def text(self) -> str:
        mark = "ok  " if self.good else "FAIL"
        return (
            f"{mark} {self.server:22} {self.tool:34} {self.status:18} "
            f"{self.millis:7} ms  retry {self.retries}  trust {self.trust:7} "
            f"{self.untrusted} {self.note}"
        )


def row_of(server: str, call: invoke.ToolCall, expected: str, result) -> Row:
    return Row(
        server=server,
        tool=call.tool,
        expected=expected,
        status=result.status,
        millis=result.duration_ms,
        retries=result.retry_attempt,
        trust=result.trust.status if result.trust else "-",
        untrusted=",".join(result.untrusted_fields) or "-",
        note=result.error.code if result.error else "",
    )


def stranger(server: str) -> str | None:
    """An agent of servers.yml that has no grant to this server, if there is one."""
    scopes = cm.load_scopes()
    agents = sorted(
        {a for scope in scopes.values() for a in scope.allowed_agents}
        - set(scopes[server].allowed_agents)
    )
    return agents[0] if agents else None


async def check_server(server: str, values: dict[str, str]) -> list[Row]:
    scope = cm.load_scopes()[server]
    calls = check.DEFAULT_CALLS[server]
    if not calls:
        return []
    rows: list[Row] = []
    agent = scope.allowed_agents[0]
    async with invoke.RunContext(agent, values=values) as ctx:
        for tool, args in calls:
            call = invoke.ToolCall(tool, args)
            result = await invoke.invoke_tool(call, ctx)
            rows.append(row_of(server, call, "ok", result))
            print(rows[-1].text(), flush=True)
        if server in REPEAT_FIRST_CALL:
            call = invoke.ToolCall(*calls[0])
            result = await invoke.invoke_tool(call, ctx)
            rows.append(row_of(server, call, "ok", result))
            rows[-1].note = rows[-1].note or "repeat call"
            print(rows[-1].text(), flush=True)
        wrong = invoke.ToolCall(calls[0][0], {"unknown_argument": 1})
        result = await invoke.invoke_tool(wrong, ctx)
        rows.append(row_of(server, wrong, "invalid_arguments", result))
        print(rows[-1].text(), flush=True)
    other = stranger(server)
    if other is not None:
        async with invoke.RunContext(other, values=values) as ctx:
            call = invoke.ToolCall(calls[0][0], calls[0][1])
            result = await invoke.invoke_tool(call, ctx)
            rows.append(row_of(server, call, "denied", result))
            print(rows[-1].text(), flush=True)
    return rows


def write_findings(rows: list[Row]) -> None:
    FINDINGS.parent.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
    lines = "\n".join(
        f"| {r.server} | {r.tool} | {r.expected} | {r.status} | {r.millis} | "
        f"{r.retries} | {r.trust} | {r.untrusted} | {r.note} |"
        for r in rows
    )
    bad = sum(1 for r in rows if not r.good)
    FINDINGS.write_text(
        "# Tool servers: invocation check\n\n"
        f"Run: {stamp}\n\n"
        "Every call goes through `invoke_tool`. `expected` is what the call must give; "
        "the wrong calls are an unknown argument and an agent without a grant.\n\n"
        "| server | tool | expected | status | ms | retries | trust | untrusted fields "
        "| error code or note |\n"
        "|---|---|---|---|---|---|---|---|---|\n"
        f"{lines}\n\n"
        f"{len(rows) - bad} of {len(rows)} calls gave the expected status.\n\n"
        f"Log lines of this run: `{call_log.LOG_DIR.relative_to(REPO_ROOT)}`.\n",
        encoding="utf-8",
    )


async def main_async() -> int:
    values = credentials.load_values()
    rows: list[Row] = []
    for server in cm.load_scopes():
        print(f"== {server}", flush=True)
        try:
            rows += await check_server(server, values)
        except Exception as exc:
            note = type(exc).__name__
            print(f"FAIL {server}: the check stopped ({note})", flush=True)
            failed = Row(server, "(check)", "ok", "server_failed", 0, 0, "-", "-", note)
            rows.append(failed)
    write_findings(rows)
    bad = [r for r in rows if not r.good]
    done = len(rows) - len(bad)
    print(f"{done} of {len(rows)} calls as expected; written {FINDINGS}")
    return 1 if bad else 0


def main() -> int:
    return anyio.run(main_async)


if __name__ == "__main__":
    sys.exit(main())
