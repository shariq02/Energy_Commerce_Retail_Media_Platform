"""Cross-ecosystem tool server: no tool yet (stdio).

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

The server is for the Orchestrator only. It has no tool because no method and no
mart for a cross-ecosystem question exist, and no identity may be invented between
ecosystems. Start: python -m ai.mcp_servers.cross_ecosystem_tools.
"""

from __future__ import annotations

from mcp.server.fastmcp import FastMCP

app = FastMCP("cross_ecosystem_tools")


if __name__ == "__main__":
    app.run(transport="stdio")
