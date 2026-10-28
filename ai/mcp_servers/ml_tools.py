"""Model tool server: three read-only tools over the model registry outputs (stdio).

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Start: python -m ai.mcp_servers.ml_tools. The server prints nothing on stdout; logs
go to stderr.
"""

from __future__ import annotations

from typing import Literal

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

from ai.mcp_servers.strict import make_strict
from ai.tools import common as cm
from ai.tools import ml

READ_ONLY = ToolAnnotations(readOnlyHint=True, openWorldHint=False)
app = FastMCP("ml_tools")


@app.tool(annotations=READ_ONLY)
def ml_read_model_card(task_id: str) -> ml.CardResult:
    """The model card (Markdown text) of one modelling task, for example
    price_daily.price. A task without a card is refused."""
    return ml.read_model_card(task_id)


@app.tool(annotations=READ_ONLY)
def ml_read_monitoring(
    task_id: str,
    kind: Literal["results", "flags", "triggers"] = "results",
    window_id: str = "*",
    limit: int = 50,
) -> cm.RowsResult:
    """Monitoring rows of one task, newest first: results (drift, null rate and
    performance checks), flags (checks that raised a warning or a flag) or triggers
    (reasons to re-evaluate a model). window_id '*' means all windows."""
    return ml.read_monitoring(task_id, kind, window_id, limit)


@app.tool(annotations=READ_ONLY)
def ml_read_predictions(
    task_id: str,
    row_key: str = "*",
    window_id: str = "*",
    model_version: int = -1,
    limit: int = 50,
) -> cm.RowsResult:
    """Stored predictions of one task, newest first. row_key '*' means all rows,
    model_version -1 means all registry versions. The table has no horizon."""
    return ml.read_predictions(task_id, row_key, window_id, model_version, limit)


make_strict(app)


if __name__ == "__main__":
    app.run(transport="stdio")
