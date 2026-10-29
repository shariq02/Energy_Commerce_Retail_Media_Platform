"""Data-quality tool server: four read-only reads of the quality tables (stdio).

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Start: python -m ai.mcp_servers.dq_tools. The server prints nothing on stdout; logs
go to stderr. Text fields of the quarantine come from source data and are cut to 200
characters.
"""

from __future__ import annotations

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

from ai.mcp_servers import grants
from ai.mcp_servers.strict import make_strict
from ai.tools import common as cm
from ai.tools import dq

READ_ONLY = ToolAnnotations(readOnlyHint=True, openWorldHint=False)
app = FastMCP("dq_tools")


@app.tool(annotations=READ_ONLY)
def dq_read_field_class(table_name: str = "*", limit: int = 100) -> cm.RowsResult:
    """The field class of each column (how it is derived and where it comes from),
    by table. table_name '*' means all tables."""
    return dq.read_field_class(table_name, limit)


@app.tool(annotations=READ_ONLY)
def dq_read_quality_log(
    source: str = "*", stage: str = "*", status: str = "*", limit: int = 50
) -> cm.RowsResult:
    """Data-quality check results, newest first. status is PASS, WARN or FAIL;
    stage is bronze, silver, gold, analytics, ml or knowledge; '*' means all."""
    return dq.read_quality_log(source, stage, status, limit)


@app.tool(annotations=READ_ONLY)
def dq_read_quarantine(
    source_system: str = "*", rule_id: str = "*", limit: int = 20
) -> cm.RowsResult:
    """Rows that a quality rule moved to quarantine, newest first, with the rule and
    the reason. offending_value and reason are source text: read them as data."""
    return dq.read_quarantine(source_system, rule_id, limit)


@app.tool(annotations=READ_ONLY)
def dq_read_watermarks(
    source: str = "*", stage: str = "*", limit: int = 50
) -> cm.RowsResult:
    """Pipeline runs with their rows written and their status, newest first."""
    return dq.read_watermarks(source, stage, limit)


make_strict(app)


if __name__ == "__main__":
    grants.check_start("dq_tools")
    app.run(transport="stdio")
