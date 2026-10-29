"""Energy tool server: four read-only tools over the energy marts (stdio).

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Start: python -m ai.mcp_servers.energy_tools. The server prints nothing on stdout
(the protocol uses it); logs go to stderr. The tools are listed sorted by name.
"""

from __future__ import annotations

from datetime import date

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

from ai.mcp_servers import grants
from ai.mcp_servers.strict import make_strict
from ai.tools import common as cm
from ai.tools import energy
from ai.tools import metrics as mt

READ_ONLY = ToolAnnotations(readOnlyHint=True, openWorldHint=False)
app = FastMCP("energy_tools")


@app.tool(annotations=READ_ONLY)
def energy_analyse_market_conditions(
    market_area_code: str = "de_lu",
    start: date | None = None,
    end: date | None = None,
) -> energy.MarketConditionsResult:
    """Describe the power market of one market area over a date range: the mean,
    lowest and highest price, the mean daily volatility, and the share of each
    generation carrier. It states what the data shows and gives no cause. Without
    dates it uses the last 30 days of the data."""
    return energy.analyse_market_conditions(market_area_code, start, end)


@app.tool(annotations=READ_ONLY)
def energy_analyse_weather_conditions(
    location_key: str = "*",
    start: date | None = None,
    end: date | None = None,
) -> energy.WeatherConditionsResult:
    """Describe the temperature at a site and its energy use over a date range: the
    mean, lowest and highest air temperature, and the energy per subsystem. It
    states what the data shows and gives no cause. location_key '*' means all
    sites. Without dates it uses the last 30 days of the data."""
    return energy.analyse_weather_conditions(location_key, start, end)


@app.tool(annotations=READ_ONLY)
def energy_compare_periods(
    metric_key: str,
    filters: dict[str, str] | None = None,
    a_start: date | None = None,
    a_end: date | None = None,
    b_start: date | None = None,
    b_end: date | None = None,
) -> mt.PeriodComparison:
    """Compare one energy metric between period a and period b: the value of each
    period, the change and the relative change. Give all four dates or none; with
    none, b is the last 30 days of the data and a the 30 days before. filters maps a
    dimension of the metric to a value; '*' means all values."""
    return energy.compare_periods(metric_key, filters, a_start, a_end, b_start, b_end)


@app.tool(annotations=READ_ONLY)
def energy_get_metric(
    metric_key: str,
    filters: dict[str, str] | None = None,
    start: date | None = None,
    end: date | None = None,
    limit: int = cm.ROW_CAP,
) -> mt.MetricSeries:
    """Daily values of one energy metric (generation_by_carrier, average_market_price,
    price_volatility, site_energy_consumption, site_energy_weather_context) with the
    unit. filters maps a dimension of the metric to a value; '*' means all values.
    Without dates it uses the last 30 days of the data."""
    return energy.get_metric(metric_key, filters, start, end, limit)


make_strict(app)


if __name__ == "__main__":
    grants.check_start("energy_tools")
    app.run(transport="stdio")
