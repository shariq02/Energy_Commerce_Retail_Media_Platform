"""The one place that reads `.env` for the tool servers, and the environment of a server.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

`spawn_env` gives the runtime the environment of one server: the server name, the
agent id, the path of the project and, for a server that reads Databricks, the
host, the HTTP path and the token of that server only. The subprocess inherits no
other secret, because the SDK passes only six plain variables besides this
environment. `.env` is read at every call, so a changed token works at the next
start. A server never reads `.env` itself. Library only.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from dotenv import dotenv_values

from ai.mcp_servers import grants
from ai.tools import common as cm

REPO_ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = REPO_ROOT / ".env"
SHARED_KEYS = ("DATABRICKS_HOST", "DATABRICKS_HTTP_PATH", "DATABRICKS_CATALOG")
REQUIRED_SHARED = ("DATABRICKS_HOST", "DATABRICKS_HTTP_PATH")


class MissingCredential(Exception):
    """A key that a server needs is not in `.env`; the message names the key only."""


def load_values(path: Path = ENV_FILE) -> dict[str, str]:
    return {k: v for k, v in dotenv_values(path).items() if v}


def token_key(server: str) -> str | None:
    return cm.TOKEN_KEYS.get(server)


def spawn_env(
    agent_id: str,
    server: str,
    values: Mapping[str, str] | None = None,
    scopes: Mapping[str, cm.ServerScope] | None = None,
) -> dict[str, str]:
    """The environment for `server` started for `agent_id`; raises GrantDenied."""
    grants.require(agent_id, server, scopes)
    values = load_values() if values is None else values
    env = {
        "PYTHONPATH": str(REPO_ROOT),
        cm.SERVER_KEY: server,
        cm.AGENT_KEY: agent_id,
    }
    key = token_key(server)
    if key is not None:
        missing = [k for k in (*REQUIRED_SHARED, key) if not values.get(k)]
        if missing:
            raise MissingCredential(f"{server}: .env has no {', '.join(missing)}")
        env.update({k: values[k] for k in SHARED_KEYS if values.get(k)})
        env[key] = values[key]
    return env
