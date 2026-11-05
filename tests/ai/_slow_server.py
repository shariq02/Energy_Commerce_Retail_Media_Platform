"""A tool server whose one tool is slow: used to measure the client timeout.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Start: python -m tests.ai._slow_server (stdio). The tool sleeps longer than any
limit of the tests. It reads no data and needs no secret.
"""

from __future__ import annotations

import anyio
from mcp.server.fastmcp import FastMCP

app = FastMCP("slow_tools")


@app.tool()
async def slow_tool() -> str:
    """Sleep for a long time, then answer."""
    await anyio.sleep(60)
    return "late"


if __name__ == "__main__":
    app.run(transport="stdio")
