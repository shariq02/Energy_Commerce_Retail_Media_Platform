"""Metric bindings and the metric tools shared by the energy and commerce servers.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

`metrics.yml` binds each metric to a mart. For each metric this module builds the
fixed SQL (a series, a comparison of two periods) and registers it with the scope of
its server at import. A caller filters on the dimensions of the metric; '*' means all
values. The first query on a mart reads its date range, which is kept for the life
of the process; a call without dates uses the last 30 days of the mart. Library only.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict

from ai.tools import common as cm

METRICS_FILE = Path(__file__).with_name("metrics.yml")
PERIODS = ("sum", "mean", "weighted_mean")
ANY = "*"
TRUST_PENDING = "trust is not computed yet; it is added with the invocation layer"
_IDENT = re.compile(r"^[a-z][a-z0-9_]*$")
_VALUE = re.compile(r"^(?:\*|[A-Za-z0-9_.:|\- ]{1,100})$")


@dataclass(frozen=True)
class Metric:
    key: str
    ecosystem: str
    schema: str
    table: str
    value: str
    unit: str
    period: str
    dimensions: tuple[str, ...]
    source_system: str
    weight: str | None = None
    one_row_per: tuple[str, ...] = ()
    prefer_largest: str | None = None
    default_filters: dict[str, str] = field(default_factory=dict)

    @property
    def mart(self) -> str:
        return f"{self.schema}.{self.table}"


def _metric(ecosystem: str, key: str, item: dict) -> Metric:
    schema, _, table = item["mart"].partition(".")
    names = [
        item["value"],
        item.get("weight") or "x",
        *item["dimensions"],
        *item.get("one_row_per", []),
        item.get("prefer_largest") or "x",
    ]
    if schema != f"{ecosystem}_analytics" or not table:
        raise cm.ScopeError(f"{key}: mart {item['mart']} is not an {ecosystem} mart")
    if not _IDENT.match(key) or not all(_IDENT.match(n) for n in [table, *names]):
        raise cm.ScopeError(f"{key}: a name is not a plain identifier")
    if item["period"] not in PERIODS:
        raise cm.ScopeError(f"{key}: period {item['period']} is not known")
    if item["period"] == "weighted_mean" and not item.get("weight"):
        raise cm.ScopeError(f"{key}: a weighted mean needs a weight column")
    defaults = item.get("default_filters", {})
    if set(defaults) - set(item["dimensions"]):
        raise cm.ScopeError(f"{key}: a default filter is not a dimension")
    if item.get("prefer_largest") and not item.get("one_row_per"):
        raise cm.ScopeError(f"{key}: prefer_largest needs one_row_per")
    return Metric(
        key=key,
        ecosystem=ecosystem,
        schema=schema,
        table=table,
        value=item["value"],
        unit=item["unit"],
        period=item["period"],
        dimensions=tuple(item["dimensions"]),
        source_system=item["source_system"],
        weight=item.get("weight"),
        one_row_per=tuple(item.get("one_row_per", [])),
        prefer_largest=item.get("prefer_largest"),
        default_filters=dict(defaults),
    )


def load_metrics(path: Path = METRICS_FILE) -> dict[str, dict[str, Metric]]:
    """Ecosystem -> metric key -> binding; a malformed binding raises ScopeError."""
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    return {
        ecosystem: {key: _metric(ecosystem, key, item) for key, item in items.items()}
        for ecosystem, items in data.items()
    }


# --- fixed SQL ------------------------------------------------------------------


def _source(metric: Metric) -> str:
    table = f"[[{metric.mart}]]"
    if metric.prefer_largest is None:
        return f"{table} m"
    keys = ", ".join(metric.one_row_per)
    return (
        "(SELECT * FROM (SELECT t.*, ROW_NUMBER() OVER ("
        f"PARTITION BY {keys} ORDER BY t.{metric.prefer_largest} DESC) AS rn "
        f"FROM {table} t) WHERE rn = 1) m"
    )


def _filters(metric: Metric) -> str:
    return "".join(f" AND (:f_{d} = '*' OR m.{d} = :f_{d})" for d in metric.dimensions)


def _aggregate(metric: Metric) -> str:
    value = f"m.{metric.value}"
    if metric.period == "sum":
        return f"SUM({value})"
    if metric.period == "mean":
        return f"AVG({value})"
    weight = f"m.{metric.weight}"
    return f"SUM({value} * {weight}) / NULLIF(SUM({weight}), 0)"


def series_sql(metric: Metric) -> str:
    dims = "".join(f", m.{d} AS {d}" for d in metric.dimensions)
    order = ", ".join(["m.local_date", *(f"m.{d}" for d in metric.dimensions)])
    return (
        f"SELECT m.local_date AS local_date{dims}, m.{metric.value} AS value "
        f"FROM {_source(metric)} "
        f"WHERE m.local_date BETWEEN :range_start AND :range_end{_filters(metric)} "
        f"ORDER BY {order}"
    )


def period_sql(metric: Metric) -> str:
    def one(tag: str) -> str:
        return (
            f"SELECT '{tag}' AS period, {_aggregate(metric)} AS value, "
            "COUNT(DISTINCT m.local_date) AS days "
            f"FROM {_source(metric)} "
            f"WHERE m.local_date BETWEEN :{tag}_start AND :{tag}_end{_filters(metric)}"
        )

    return f"{one('a')} UNION ALL {one('b')}"


def bounds_sql(mart: str) -> str:
    return (
        "SELECT MIN(m.local_date) AS first_date, MAX(m.local_date) AS last_date "
        f"FROM [[{mart}]] m"
    )


def filter_names(metric: Metric) -> tuple[str, ...]:
    return tuple(f"f_{d}" for d in metric.dimensions)


# --- results --------------------------------------------------------------------


class MetricPoint(BaseModel):
    model_config = ConfigDict(extra="forbid")

    local_date: date
    dimensions: dict[str, str | None]
    value: float | None


class MetricSeries(cm.TrustedToolResult):
    metric_key: str
    unit: str
    data: list[MetricPoint]


class PeriodValue(BaseModel):
    model_config = ConfigDict(extra="forbid")

    period: str
    start: date
    end: date
    value: float | None
    days: int


class PeriodComparison(cm.TrustedToolResult):
    metric_key: str
    unit: str
    aggregation: str
    data: list[PeriodValue]
    change: float | None = None
    relative_change: float | None = None


def number(value: Any) -> float | None:
    return None if value is None else float(value)


def check_text(name: str, value: str) -> str:
    if not _VALUE.match(value):
        raise cm.ToolFailure(
            "invalid_argument",
            f"{name} has characters that are not allowed",
            "use letters, digits, space, '_', '-', '.', ':', '|', or '*' for all",
        )
    return value


# --- the tools of one ecosystem -------------------------------------------------


class Domain:
    """The metric tools of one ecosystem, on the scope of one server."""

    def __init__(
        self,
        ecosystem: str,
        scope: cm.ServerScope,
        bindings: dict[str, dict[str, Metric]],
    ) -> None:
        self.ecosystem = ecosystem
        self.scope = scope
        self.metrics = bindings[ecosystem]
        self._other = {
            key for eco, items in bindings.items() if eco != ecosystem for key in items
        }
        self.series_templates = {
            key: scope.register(
                cm.SqlTemplate(
                    f"{key} series",
                    series_sql(m),
                    ("range_start", "range_end", *filter_names(m)),
                )
            )
            for key, m in self.metrics.items()
        }
        self.period_templates = {
            key: scope.register(
                cm.SqlTemplate(
                    f"{key} periods",
                    period_sql(m),
                    ("a_start", "a_end", "b_start", "b_end", *filter_names(m)),
                )
            )
            for key, m in self.metrics.items()
        }
        self.bounds_templates = {
            mart: scope.register(cm.SqlTemplate(f"bounds {mart}", bounds_sql(mart)))
            for mart in sorted({m.mart for m in self.metrics.values()})
        }
        self._bounds: dict[str, tuple[date, date]] = {}
        definitions = (
            "SELECT metric_key, source_mart "
            f"FROM [[{ecosystem}_semantic.metric_definition]]"
        )
        self.definitions = scope.register(
            cm.SqlTemplate("metric definitions", definitions)
        )

    def metric(self, key: str) -> Metric:
        if key in self.metrics:
            return self.metrics[key]
        if key in self._other:
            raise cm.ToolFailure(
                "wrong_ecosystem",
                f"{key} is not a {self.ecosystem} metric",
                f"the {self.ecosystem} metrics are {sorted(self.metrics)}",
            )
        raise cm.ToolFailure(
            "unknown_metric",
            f"{key} is not a metric",
            f"the {self.ecosystem} metrics are {sorted(self.metrics)}",
        )

    def bounds(self, mart: str) -> tuple[date, date]:
        """The first and last date of a mart, read once per process."""
        if mart not in self._bounds:
            result = cm.get_warehouse().run(self.scope, self.bounds_templates[mart])
            row = result.rows[0] if result.rows else {}
            if row.get("first_date") is None:
                raise cm.ToolFailure("range_outside_data", f"{mart} holds no rows", "")
            self._bounds[mart] = (row["first_date"], row["last_date"])
        return self._bounds[mart]

    def filter_params(
        self, metric: Metric, filters: dict[str, str] | None
    ) -> tuple[dict[str, str], list[str]]:
        given = dict(filters or {})
        unknown = set(given) - set(metric.dimensions)
        if unknown:
            raise cm.ToolFailure(
                "invalid_argument",
                f"{sorted(unknown)} are not dimensions of {metric.key}",
                f"the dimensions are {list(metric.dimensions)}",
            )
        warnings = []
        params = {}
        for dim in metric.dimensions:
            if dim in given:
                value = check_text(dim, str(given[dim]))
            elif dim in metric.default_filters:
                value = metric.default_filters[dim]
                warnings.append(
                    f"{dim} is filtered to '{value}'; give {dim}='*' to see all values"
                )
            else:
                value = ANY
            params[f"f_{dim}"] = value
        return params, warnings

    def span(
        self, mart: str, start: date | None, end: date | None
    ) -> tuple[date, date, date]:
        """The dates of a call and the last date of the mart."""
        first, last = self.bounds(mart)
        if start is None and end is None:
            start, end = cm.default_range(last)
        elif start is None or end is None:
            raise cm.ToolFailure(
                "invalid_argument", "give both start and end, or neither", ""
            )
        if start > end:
            raise cm.ToolFailure("invalid_argument", "start is after end", "")
        if end < first or start > last:
            raise cm.ToolFailure(
                "range_outside_data",
                f"{mart} covers {first} to {last}",
                "choose dates inside that range",
            )
        return start, end, last

    def _notes(self, metric: Metric, warnings: list[str]) -> list[str]:
        notes = [*warnings]
        if metric.prefer_largest:
            notes.append(
                "where the mart holds a daily row and quarter-hour rows for the same "
                "day, the daily row is used"
            )
        notes.append(TRUST_PENDING)
        return notes

    def series(
        self,
        key: str,
        filters: dict[str, str] | None,
        start: date | None,
        end: date | None,
        limit: int,
    ) -> MetricSeries:
        metric = self.metric(key)
        if not 1 <= limit <= cm.ROW_CAP_MAX:
            raise cm.ToolFailure(
                "invalid_argument", f"limit is outside 1 to {cm.ROW_CAP_MAX}", ""
            )
        params, warnings = self.filter_params(metric, filters)
        start, end, last = self.span(metric.mart, start, end)
        result = cm.get_warehouse().run(
            self.scope,
            self.series_templates[key],
            {"range_start": start, "range_end": end, **params},
            row_cap=limit,
        )
        points = [
            MetricPoint(
                local_date=row["local_date"],
                dimensions={d: row[d] for d in metric.dimensions},
                value=number(row["value"]),
            )
            for row in result.rows
        ]
        notes = self._notes(metric, warnings)
        if result.truncated:
            notes.append(f"the result is cut at {limit} rows; narrow the filter")
        if not points:
            notes.append("no rows for this filter and range")
        return MetricSeries(
            metric_key=key,
            unit=metric.unit,
            data=points,
            source_tables=[metric.mart],
            as_of=str(last),
            row_count=len(points),
            truncated=result.truncated,
            warnings=notes,
            trust=cm.Trust(status="unknown"),
        )

    def compare(
        self,
        key: str,
        filters: dict[str, str] | None,
        a: tuple[date | None, date | None],
        b: tuple[date | None, date | None],
    ) -> PeriodComparison:
        metric = self.metric(key)
        params, warnings = self.filter_params(metric, filters)
        _first, last = self.bounds(metric.mart)
        if all(v is None for v in (*a, *b)):
            b = cm.default_range(last)
            a_end = b[0] - timedelta(days=1)
            a = cm.default_range(a_end)
        elif any(v is None for v in (*a, *b)):
            raise cm.ToolFailure(
                "invalid_argument",
                "give all four dates of the two periods, or none",
                "",
            )
        if a[0] > a[1] or b[0] > b[1]:
            raise cm.ToolFailure(
                "invalid_argument", "a period starts after it ends", ""
            )
        result = cm.get_warehouse().run(
            self.scope,
            self.period_templates[key],
            {
                "a_start": a[0],
                "a_end": a[1],
                "b_start": b[0],
                "b_end": b[1],
                **params,
            },
        )
        by_tag = {row["period"]: row for row in result.rows}
        spans = {"a": a, "b": b}
        data = [
            PeriodValue(
                period=tag,
                start=spans[tag][0],
                end=spans[tag][1],
                value=number(by_tag[tag]["value"]) if tag in by_tag else None,
                days=int(by_tag[tag]["days"]) if tag in by_tag else 0,
            )
            for tag in ("a", "b")
        ]
        notes = self._notes(metric, warnings)
        if metric.period == "mean":
            notes.append("each period value is the mean of the daily values")
        if all(item.days == 0 for item in data):
            notes.append("no rows in either period for this filter")
        value_a, value_b = data[0].value, data[1].value
        change = None if value_a is None or value_b is None else value_b - value_a
        relative = None if change is None or not value_a else change / abs(value_a)
        return PeriodComparison(
            metric_key=key,
            unit=metric.unit,
            aggregation=metric.period,
            data=data,
            change=change,
            relative_change=relative,
            source_tables=[metric.mart],
            as_of=str(last),
            row_count=len(data),
            warnings=notes,
            trust=cm.Trust(status="unknown"),
        )

    def verify_bindings(self) -> list[str]:
        """Problems between metrics.yml and the live metric_definition rows."""
        result = cm.get_warehouse().run(self.scope, self.definitions)
        live = {row["metric_key"]: row["source_mart"] for row in result.rows}
        bound = set(self.metrics)
        problems = [
            f"{k}: in metrics.yml, not in metric_definition"
            for k in sorted(bound - set(live))
        ]
        problems += [
            f"{k}: in metric_definition, not in metrics.yml"
            for k in sorted(set(live) - bound)
        ]
        problems += [
            f"{k}: mart {self.metrics[k].table} differs from {live[k]}"
            for k in sorted(set(live) & set(self.metrics))
            if live[k] != self.metrics[k].table
        ]
        return problems
