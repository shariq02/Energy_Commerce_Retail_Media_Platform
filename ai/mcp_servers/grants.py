"""Which agent may start which tool server: the grant table of servers.yml.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

The runtime asks `require` before it starts a server, and the server asks
`check_start` when it starts, so a server never runs for an agent that has no grant.
The agent id is set by the runtime in the environment of the subprocess; a model
cannot set it. Library only.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Mapping

from ai.tools import common as cm


class GrantDenied(Exception):
    """An agent asked for a server that its grant does not list."""


def is_allowed(
    agent_id: str, server: str, scopes: Mapping[str, cm.ServerScope] | None = None
) -> bool:
    scope = (scopes or cm.load_scopes()).get(server)
    return scope is not None and agent_id in scope.allowed_agents


def require(
    agent_id: str, server: str, scopes: Mapping[str, cm.ServerScope] | None = None
) -> None:
    if not is_allowed(agent_id, server, scopes):
        raise GrantDenied(f"{agent_id or 'no agent'} may not start {server}")


def check_start(server: str, env: Mapping[str, str] | None = None) -> None:
    """Stop the process with exit code 2 when the agent of the environment has no grant."""
    agent = (env if env is not None else os.environ).get(cm.AGENT_KEY, "")
    try:
        require(agent, server)
    except GrantDenied as exc:
        sys.stderr.write(f"{exc}\n")
        raise SystemExit(2) from None
