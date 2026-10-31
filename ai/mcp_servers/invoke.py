"""One function to call a tool: grant, budget, time limit, retry, log, evidence.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

`invoke_tool(call, ctx)` never raises to the caller. It checks the grant of the agent
and the call budget, calls the tool with a time limit of 60 s (120 s for
`knowledge_retrieve`, whose first call loads the models), retries only a
transient failure (a timeout or a server that stopped or did not start) at most twice
with a wait of 1 s and 3 s, and only for a tool with `readOnlyHint`. It writes one log
line per attempt and returns a `ToolEvidence`. A run context keeps one client session
per server for one question, so a server starts once; a session that timed out or
crashed is dropped and the next attempt starts the server again. When a server starts,
the hash of each running tool definition is compared with the contract lock; a
difference gives `contract_violation` (code `definition_changed`) and no call is made.

Cost of a call: one client round trip; a server start adds one `tools/list`. Limits:
60 s to start a server, 60 s per call, three attempts at most, 150 s for one
`invoke_tool` including retries, 300 s of call time for one question (the budget).
Library only; `invoke_check` is the command.
"""

from __future__ import annotations

import sys
import time
import uuid
from collections.abc import Awaitable, Callable, Mapping
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any, Self

import anyio
from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client
from mcp.shared.exceptions import McpError

from ai.mcp_servers import call_log, contracts, credentials, evidence, grants
from ai.tools import common as cm

REPO_ROOT = Path(__file__).resolve().parents[2]
START_SECONDS = 60
CALL_SECONDS = 60
# A tool whose first call loads a model gets a longer limit than the default.
CALL_SECONDS_BY_TOOL = {"knowledge_retrieve": 120}
BACKOFF_SECONDS = (1.0, 3.0)
MAX_ATTEMPTS = 3
CALL_DEADLINE_SECONDS = 150
QUESTION_SECONDS = 300
MIN_RETRY_SECONDS = 10
MAX_ARGUMENT_CHARS = 10_000
REQUEST_TIMEOUT_CODE = 408
CONNECTION_CLOSED_CODE = -32000
CLOSED_ERRORS = (
    anyio.ClosedResourceError,
    anyio.BrokenResourceError,
    anyio.EndOfStream,
    EOFError,
    OSError,
)


@dataclass(frozen=True)
class ToolCall:
    tool: str
    arguments: dict[str, Any] = field(default_factory=dict)


class CallBudget:
    """What one question may spend: calls in total, calls per tool and call time."""

    def __init__(
        self, total: int = 20, per_tool: int = 5, seconds: float = QUESTION_SECONDS
    ) -> None:
        self.total = total
        self.per_tool = per_tool
        self.seconds = seconds
        self.spent = 0.0
        self.used = 0
        self.by_tool: dict[str, int] = {}

    def left(self) -> float:
        return self.seconds - self.spent

    def take(self, tool: str) -> bool:
        if self.left() <= 0 or self.used >= self.total:
            return False
        if self.by_tool.get(tool, 0) >= self.per_tool:
            return False
        self.used += 1
        self.by_tool[tool] = self.by_tool.get(tool, 0) + 1
        return True


Opener = Callable[["RunContext", str, AsyncExitStack], Awaitable[Any]]


class DefinitionMismatch(Exception):
    """A running tool differs from the contract lock; `names` are the tools."""

    def __init__(self, names: list[str]) -> None:
        self.names = [call_log.safe_name(name) for name in names]
        super().__init__(", ".join(self.names[:5]))


def live_hashes(tools: Any) -> dict[str, str]:
    """Tool name -> definition hash, from the tools a running server lists. The form
    is the one of the lock (`contracts.current_definitions`)."""
    found: dict[str, str] = {}
    for tool in tools:
        dumped = tool.model_dump(mode="json", exclude_none=True)
        definition = {k: dumped[k] for k in contracts.DEFINITION_KEYS if k in dumped}
        found[tool.name] = contracts.definition_hash(definition)
    return found


def definition_mismatches(
    server: str, tools: Any, lock: Mapping[str, dict]
) -> list[str]:
    """The tools of a server whose running definition differs from the lock: changed,
    missing, or listed by the server and unknown to the lock."""
    live = live_hashes(tools)
    wanted = {n: e["definition_hash"] for n, e in lock.items() if e["server"] == server}
    changed = {name for name, digest in wanted.items() if live.get(name) != digest}
    return sorted(changed | (set(live) - set(wanted)))


async def stdio_opener(ctx: RunContext, server: str, stack: AsyncExitStack) -> Any:
    """Start a server as a subprocess on stdio and open a client session to it."""
    env = credentials.spawn_env(ctx.agent, server, ctx.values)
    params = StdioServerParameters(
        command=sys.executable,
        args=["-m", f"ai.mcp_servers.{server}"],
        env=env,
        cwd=REPO_ROOT,
    )
    read, write = await stack.enter_async_context(stdio_client(params))
    session = await stack.enter_async_context(ClientSession(read, write))
    with anyio.fail_after(START_SECONDS):
        await session.initialize()
        listed = (await session.list_tools()).tools
    changed = definition_mismatches(server, listed, ctx.lock)
    if changed:
        raise DefinitionMismatch(changed)
    return session


class RunContext:
    """The state of one question: the agent, the budget and the open sessions.

    Use it as `async with RunContext(agent) as ctx`; the sessions close on exit."""

    def __init__(
        self,
        agent: str,
        *,
        trace_id: str | None = None,
        budget: CallBudget | None = None,
        values: Mapping[str, str] | None = None,
        opener: Opener = stdio_opener,
        log_dir: Path = call_log.LOG_DIR,
        sleep: Callable[[float], Awaitable[None]] = anyio.sleep,
        scopes: Mapping[str, cm.ServerScope] | None = None,
        lock: dict | None = None,
    ) -> None:
        self.agent = agent
        self.trace_id = trace_id or uuid.uuid4().hex
        self.budget = budget or CallBudget()
        self.values = values
        self.log_dir = log_dir
        self.sleep = sleep
        self.scopes = scopes or cm.load_scopes()
        self.lock = (lock if lock is not None else contracts.read_lock())["tools"]
        self._opener = opener
        self._sessions: dict[str, Any] = {}
        self._stacks: dict[str, AsyncExitStack] = {}
        self._locks: dict[str, anyio.Lock] = {}

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        for server in list(self._stacks):
            await self.drop(server)

    async def session(self, server: str) -> Any:
        """The open session of a server; one lock per server, so two calls at the
        same time start the server once."""
        lock = self._locks.setdefault(server, anyio.Lock())
        async with lock:
            if server not in self._sessions:
                stack = AsyncExitStack()
                self._stacks[server] = stack
                try:
                    self._sessions[server] = await self._opener(self, server, stack)
                except BaseException:
                    await self.drop(server)
                    raise
            return self._sessions[server]

    async def drop(self, server: str) -> None:
        """Close and forget the session of a server (it timed out or stopped)."""
        self._sessions.pop(server, None)
        stack = self._stacks.pop(server, None)
        if stack is not None:
            try:
                await stack.aclose()
            except Exception as exc:
                sys.stderr.write(f"closing {server}: {type(exc).__name__}\n")


@dataclass
class Attempt:
    evidence: evidence.ToolEvidence
    transient: bool = False


def _contract_of(ctx: RunContext, tool: str) -> dict | None:
    return ctx.lock.get(tool)


def _needs_trust(entry: dict) -> bool:
    schema = entry["definition"].get("outputSchema") or {}
    return "trust" in (schema.get("required") or [])


def _read_only(entry: dict) -> bool:
    return bool((entry["definition"].get("annotations") or {}).get("readOnlyHint"))


def _failure(
    status: str,
    tool: str,
    code: str,
    message: str,
    hint: str = "",
    *,
    transient: bool = False,
) -> Attempt:
    return Attempt(
        evidence.ToolEvidence(
            status=status,
            tool=tool,
            error=evidence.make_error(code, message, hint),
        ),
        transient,
    )


def _timeout(tool: str) -> Attempt:
    return _failure(
        "timeout", tool, "timeout", "the call took too long", transient=True
    )


def _first_text(result: Any) -> str:
    return next((c.text for c in result.content if hasattr(c, "text")), "")


def _from_result(call: ToolCall, result: Any, needs_trust: bool) -> Attempt:
    """The evidence of a result that arrived."""
    if result.isError:
        status, error = evidence.parse_error(_first_text(result))
        return Attempt(
            evidence.ToolEvidence(status=status, tool=call.tool, error=error)
        )
    structured = result.structuredContent
    problem = evidence.check_result(structured, needs_trust)
    if problem is not None:
        return Attempt(
            evidence.ToolEvidence(
                status="contract_violation", tool=call.tool, error=problem
            )
        )
    data, untrusted = evidence.wrap_untrusted(call.tool, structured)
    trust = cm.Trust.model_validate(structured["trust"]) if needs_trust else None
    return Attempt(
        evidence.ToolEvidence(
            status="ok",
            tool=call.tool,
            data=data,
            as_of=structured.get("as_of"),
            trust=trust,
            truncated=bool(structured.get("truncated")),
            row_count=structured.get("row_count"),
            untrusted_fields=untrusted,
        )
    )


async def _attempt(
    call: ToolCall, ctx: RunContext, server: str, needs_trust: bool, deadline: float
) -> Attempt:
    """One try: open the session if needed, call, classify. Never raises. The call
    waits at most 60 s and never past the deadline of the whole `invoke_tool`."""
    try:
        session = await ctx.session(server)
    except credentials.MissingCredential as exc:
        return _failure("server_failed", call.tool, "missing_credential", str(exc))
    except DefinitionMismatch as exc:
        return _failure(
            "contract_violation",
            call.tool,
            "definition_changed",
            "a running tool differs from the contract lock",
            ", ".join(exc.names[:5]),
        )
    except Exception as exc:
        return _failure(
            "server_failed",
            call.tool,
            "start_failed",
            f"the server did not start ({type(exc).__name__})",
            transient=True,
        )
    allowed = CALL_SECONDS_BY_TOOL.get(call.tool, CALL_SECONDS)
    limit = max(1.0, min(allowed, deadline - time.monotonic()))
    try:
        with anyio.fail_after(limit + 5):
            result = await session.call_tool(
                call.tool,
                call.arguments,
                read_timeout_seconds=timedelta(seconds=limit),
            )
    except TimeoutError:
        await ctx.drop(server)
        return _timeout(call.tool)
    except McpError as exc:
        await ctx.drop(server)
        code = exc.error.code
        if code == REQUEST_TIMEOUT_CODE:
            return _timeout(call.tool)
        if code == CONNECTION_CLOSED_CODE:
            return _failure(
                "server_failed",
                call.tool,
                "server_stopped",
                "the server stopped",
                transient=True,
            )
        return _failure("tool_error", call.tool, "protocol_error", "the server refused")
    except CLOSED_ERRORS as exc:
        await ctx.drop(server)
        return _failure(
            "server_failed",
            call.tool,
            "server_stopped",
            f"the server stopped ({type(exc).__name__})",
            transient=True,
        )
    except RuntimeError as exc:
        text = str(exc)
        if "structured content" in text or "Invalid schema" in text:
            return _failure(
                "contract_violation",
                call.tool,
                "contract_violation",
                "the result does not match the output schema",
            )
        await ctx.drop(server)
        return _failure("server_failed", call.tool, "client_error", "the client failed")
    except Exception as exc:
        await ctx.drop(server)
        return _failure(
            "server_failed",
            call.tool,
            "client_error",
            f"the client failed ({type(exc).__name__})",
        )
    try:
        return _from_result(call, result, needs_trust)
    except (ValueError, TypeError, KeyError, AttributeError):
        return _failure(
            "contract_violation",
            call.tool,
            "contract_violation",
            "the result has a field of the wrong type",
        )


def _log(
    call: ToolCall,
    ctx: RunContext,
    result: evidence.ToolEvidence,
    server: str,
    entry: dict | None,
    attempt: int,
) -> bool:
    """Write the log line of one attempt; False when the line could not be written."""
    line = call_log.LogLine(
        trace_id=ctx.trace_id,
        timestamp=call_log.now(),
        agent=call_log.safe_name(ctx.agent),
        mcp_server=server,
        tool_name=call_log.safe_name(call.tool),
        argument_hash=call_log.argument_hash(call.arguments),
        arguments=call_log.logged_arguments(call.tool, call.arguments),
        result_status=result.status,
        duration_ms=result.duration_ms,
        retry_attempt=attempt,
        tool_version=entry["version"] if entry else None,
        definition_hash=entry["definition_hash"] if entry else None,
        error_code=result.error.code if result.error else None,
        row_count=result.row_count,
    )
    try:
        call_log.write_line(line, ctx.log_dir)
    except OSError as exc:
        sys.stderr.write(f"tool call log not written: {type(exc).__name__}\n")
        return False
    return True


def _logged(
    call: ToolCall,
    ctx: RunContext,
    result: evidence.ToolEvidence,
    server: str,
    entry: dict | None,
    attempt: int,
) -> evidence.ToolEvidence:
    """The evidence after its log line; `log_written` is False when the write failed."""
    if _log(call, ctx, result, server, entry, attempt):
        return result
    return result.model_copy(update={"log_written": False})


def _stamp(
    result: evidence.ToolEvidence,
    server: str,
    entry: dict | None,
    attempt: int,
    started: float,
) -> evidence.ToolEvidence:
    return result.model_copy(
        update={
            "server": server or None,
            "tool_version": entry["version"] if entry else None,
            "definition_hash": entry["definition_hash"] if entry else None,
            "retry_attempt": attempt,
            "duration_ms": int((time.monotonic() - started) * 1000),
        }
    )


async def invoke_tool(call: ToolCall, ctx: RunContext) -> evidence.ToolEvidence:
    """Call one tool for the agent of the context; always returns evidence."""
    entry = _contract_of(ctx, call.tool)
    server = entry["server"] if entry else ""
    started = time.monotonic()
    refusal: Attempt | None = None
    if entry is None:
        refusal = _failure(
            "invalid_arguments",
            call_log.safe_name(call.tool),
            "unknown_tool",
            "no such tool",
        )
    elif not grants.is_allowed(ctx.agent, server, ctx.scopes):
        refusal = _failure(
            "denied",
            call.tool,
            "denied",
            f"{ctx.agent or 'no agent'} may not use {server}",
        )
    elif len(call_log.canonical(call.arguments)) > MAX_ARGUMENT_CHARS:
        refusal = _failure(
            "invalid_arguments",
            call.tool,
            "arguments_too_large",
            f"the arguments are longer than {MAX_ARGUMENT_CHARS} characters",
        )
    elif not ctx.budget.take(call.tool):
        refusal = _failure(
            "budget_exhausted",
            call.tool,
            "budget_exhausted",
            "the call budget of this question is used up",
            f"at most {ctx.budget.total} calls, {ctx.budget.per_tool} per tool "
            f"and {ctx.budget.seconds:g} s of call time",
        )
    if refusal is not None:
        result = _stamp(refusal.evidence, server, entry, 0, started)
        return _logged(call, ctx, result, server, entry, 0)
    needs_trust = _needs_trust(entry)
    attempts = MAX_ATTEMPTS if _read_only(entry) else 1
    began = time.monotonic()
    deadline = began + min(CALL_DEADLINE_SECONDS, ctx.budget.left())
    try:
        for number in range(attempts):
            started = time.monotonic()
            attempt = await _attempt(call, ctx, server, needs_trust, deadline)
            result = _stamp(attempt.evidence, server, entry, number, started)
            result = _logged(call, ctx, result, server, entry, number)
            last = number + 1 >= attempts
            wait = 0.0 if last else BACKOFF_SECONDS[number]
            if last or not attempt.transient:
                return result
            if deadline - time.monotonic() - wait < MIN_RETRY_SECONDS:
                return result
            await ctx.sleep(wait)
        return result
    finally:
        ctx.budget.spent += time.monotonic() - began
