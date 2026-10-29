"""Commerce tool server: two read-only tools over the commerce marts (stdio).

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Start: python -m ai.mcp_servers.commerce_tools. The server prints nothing on stdout;
logs go to stderr.
"""

from __future__ import annotations

from datetime import date

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

from ai.mcp_servers import grants
from ai.mcp_servers.strict import make_strict
from ai.tools import commerce
from ai.tools import common as cm
from ai.tools import metrics as mt

READ_ONLY = ToolAnnotations(readOnlyHint=True, openWorldHint=False)
app = FastMCP("commerce_tools")


@app.tool(annotations=READ_ONLY)
def commerce_compare_periods(
    metric_key: str,
    filters: dict[str, str] | None = None,
    a_start: date | None = None,
    a_end: date | None = None,
    b_start: date | None = None,
    b_end: date | None = None,
) -> mt.PeriodComparison:
    """Compare one commerce metric between period a and period b: the value of each
    period, the change and the relative change. Give all four dates or none; with
    none, b is the last 30 days of the data and a the 30 days before. filters maps a
    dimension of the metric to a value; '*' means all values."""
    return commerce.compare_periods(metric_key, filters, a_start, a_end, b_start, b_end)


@app.tool(annotations=READ_ONLY)
def commerce_get_metric(
    metric_key: str,
    filters: dict[str, str] | None = None,
    start: date | None = None,
    end: date | None = None,
    limit: int = cm.ROW_CAP,
) -> mt.MetricSeries:
    """Daily values of one commerce metric (ga4_sessions, ga4_revenue,
    rees46_event_demand) with the unit. filters maps a dimension of the metric to a
    value; '*' means all values. Without dates it uses the last 30 days of the data."""
    return commerce.get_metric(metric_key, filters, start, end, limit)


make_strict(app)


if __name__ == "__main__":
    grants.check_start("commerce_tools")
    app.run(transport="stdio")
