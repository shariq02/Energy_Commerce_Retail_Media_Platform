"""Strict input schemas for a tool server: an argument that is not listed is refused.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

The SDK builds the argument model of each tool without a rule for extra fields, so an
unknown argument is dropped silently and the schema does not say otherwise. This
module sets `extra = forbid` on each argument model and builds the schema again, so
the schema carries `additionalProperties: false` and a call with an unknown argument
fails before the tool runs. A server calls it once, after its tools are declared.
"""

from __future__ import annotations

from mcp.server.fastmcp import FastMCP


def make_strict(app: FastMCP) -> None:
    for tool in app._tool_manager.list_tools():
        model = tool.fn_metadata.arg_model
        model.model_config["extra"] = "forbid"
        model.model_rebuild(force=True)
        tool.parameters = model.model_json_schema(by_alias=True)
