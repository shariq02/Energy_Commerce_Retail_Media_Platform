"""Commerce tools: a metric and a comparison of two periods.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Both tools read only the commerce marts and the commerce semantic layer. A metric of
the energy ecosystem is refused with a typed error. Library only.
"""

from __future__ import annotations

from datetime import date

from ai.tools import common as cm
from ai.tools import metrics as mt

SERVER = "commerce_tools"
SCOPE = cm.load_scopes()[SERVER]
DOMAIN = mt.Domain("commerce", SCOPE, mt.load_metrics())


@cm.guarded
def get_metric(
    metric_key: str,
    filters: dict[str, str] | None = None,
    start: date | None = None,
    end: date | None = None,
    limit: int = cm.ROW_CAP,
) -> mt.MetricSeries:
    return DOMAIN.series(metric_key, filters, start, end, limit)


@cm.guarded
def compare_periods(
    metric_key: str,
    filters: dict[str, str] | None = None,
    a_start: date | None = None,
    a_end: date | None = None,
    b_start: date | None = None,
    b_end: date | None = None,
) -> mt.PeriodComparison:
    return DOMAIN.compare(metric_key, filters, (a_start, a_end), (b_start, b_end))
