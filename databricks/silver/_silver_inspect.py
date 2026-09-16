# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER INSPECTION LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** the reusable post-Silver inspection framework, pulled into
# MAGIC every Silver notebook with `%run ../_silver_inspect` (or
# MAGIC `%run ../../_silver_inspect`) after `_silver_common`. Definitions only --
# MAGIC no side effects at import; the caller owns `spark`. Every check is a small,
# MAGIC bounded number of aggregate passes, never a per-column scan loop -- the
# MAGIC same performance discipline as `_silver_common.py`.
# MAGIC
# MAGIC Persists to `quality.silver_inspection_log` (shared across ecosystems, same
# MAGIC pattern as `quality.quality_audit_log` / `quality.pipeline_watermarks`).
# MAGIC Inspection observes and records; it never blocks or fails a Silver write --
# MAGIC a check that cannot be computed is logged as an INFO row with a note, not
# MAGIC raised.

# COMMAND ----------

# DBTITLE 1,Imports
import re as _re

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
INSPECTION_TABLE = f"{CATALOG}.{QUALITY_SCHEMA}.silver_inspection_log"

# Columns skipped for approx_count_distinct -- expected-high-cardinality /
# free-text fields where a distinct count is not diagnostic and only adds cost.
HIGH_CARDINALITY_SKIP = {
    "source_record_id",
    "url",
    "page_location",
    "page_title",
    "search_term",
    "unique_search_term",
    "plant_name",
    "station_name",
    "offending_value",
}

# Boolean/status columns this framework recognises as DQ/trust flags by name.
_FLAG_COL_PATTERN = _re.compile(
    r"^_.*_(flag|violated|violation|outlier|suspected|ambiguous|out_of_set|conflict)$"
    r"|^is_[a-z_]+$"
    r"|_status$"
    r"|^currency_unknown$"
)

_LOG_SCHEMA = (
    "inspection_timestamp timestamp, run_id string, source_system string, "
    "component string, table_name string, check_category string, check_name string, "
    "metric string, observed_value double, expected_value double, status string, "
    "severity string, details string"
)

# COMMAND ----------


# DBTITLE 1,Row writer
def _log_rows(rows: list[tuple]) -> None:
    if not rows:
        return
    spark.createDataFrame(rows, _LOG_SCHEMA).write.format("delta").mode(
        "append"
    ).saveAsTable(INSPECTION_TABLE)


# COMMAND ----------

# DBTITLE 1,Common inspection


def inspect_table(
    df_after: DataFrame,
    silver_table: str,
    *,
    source: str,
    component: str,
    rid: str,
    key_cols: list[str] | None = None,
    df_before: DataFrame | None = None,
    extra_checks: dict | None = None,
) -> None:
    """Run the common post-Silver inspection on the just-written `df_after` and
    persist the results to `quality.silver_inspection_log`.

    `key_cols` -- the table's business grain key (not `_srid`) -- enables the
    duplicate/grain check; omit where no single business key applies.
    `df_before` -- the Bronze (or pre-transformation) read -- enables the
    input/output row-count reconciliation check; omit where not meaningful
    (e.g. a derived reference table with no 1:1 Bronze counterpart).
    `extra_checks` -- {check_name: value} for source-specific findings the
    caller already computed (see each notebook's own inspection call) -- passed
    through as `transformation_specific` rows, never invented by this function.
    """
    ts = now_utc()
    rows: list[tuple] = []
    full = f"{CATALOG}.{target_schema_for(silver_table, source=source)}.{silver_table}"
    cols = df_after.columns
    dtypes = dict(df_after.dtypes)

    def add(category, name, metric, observed, expected, status, severity, details=""):
        rows.append(
            (
                ts,
                rid,
                source,
                component,
                full,
                category,
                name,
                metric,
                None if observed is None else float(observed),
                None if expected is None else float(expected),
                status,
                severity,
                details,
            )
        )

    # Schema
    add("schema", "table_exists", "exists", 1.0, 1.0, "PASS", "INFO")
    add("schema", "column_count", "count", float(len(cols)), None, "INFO", "INFO")

    # Volume
    n_after = df_after.count()
    add("volume", "row_count", "rows", float(n_after), None, "INFO", "INFO")
    if df_before is not None:
        n_before = df_before.count()
        delta = n_after - n_before
        ratio = (n_after / n_before) if n_before else None
        vol_status = (
            "WARN" if (ratio is not None and (ratio < 0.5 or ratio > 2.0)) else "PASS"
        )
        add(
            "volume",
            "input_output_reconciliation",
            "row_delta",
            float(delta),
            float(n_before),
            vol_status,
            "WARN" if vol_status == "WARN" else "INFO",
            f"before={n_before} after={n_after}",
        )

    # Completeness (one agg pass across every column)
    if cols:
        null_exprs = [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in cols]
        null_row = df_after.agg(*null_exprs).first()
        if null_row and n_after:
            for c in cols:
                rate = (null_row[c] or 0) / n_after
                add(
                    "completeness",
                    f"null_rate:{c}",
                    "null_rate",
                    rate,
                    None,
                    "INFO",
                    "INFO",
                )

    # Cardinality (one agg pass, skipping known-high-cardinality columns)
    card_cols = [c for c in cols if c not in HIGH_CARDINALITY_SKIP]
    if card_cols:
        card_exprs = [F.approx_count_distinct(F.col(c)).alias(c) for c in card_cols]
        card_row = df_after.agg(*card_exprs).first()
        for c in card_cols:
            add(
                "cardinality",
                f"distinct:{c}",
                "approx_distinct",
                float(card_row[c]),
                None,
                "INFO",
                "INFO",
            )

    # Grain / duplicate-key check
    if key_cols:
        dup_n = df_after.groupBy(*key_cols).count().filter(F.col("count") > 1).count()
        grain_status = "WARN" if dup_n else "PASS"
        add(
            "grain",
            "duplicate_keys",
            "count",
            float(dup_n),
            0.0,
            grain_status,
            "WARN" if dup_n else "INFO",
            f"key_cols={key_cols}",
        )

    # Numeric behaviour (one agg pass over numeric columns)
    numeric_types = {"double", "float", "int", "bigint", "smallint", "decimal"}
    numeric_cols = [c for c in cols if dtypes.get(c) in numeric_types]
    if numeric_cols:
        stat_exprs = []
        for c in numeric_cols:
            stat_exprs += [
                F.min(c).alias(f"{c}__min"),
                F.max(c).alias(f"{c}__max"),
                F.sum((F.col(c) == 0).cast("int")).alias(f"{c}__zero"),
                F.sum((F.col(c) < 0).cast("int")).alias(f"{c}__neg"),
            ]
        stat_row = df_after.agg(*stat_exprs).first()
        for c in numeric_cols:
            add(
                "numeric",
                f"min:{c}",
                "min",
                stat_row[f"{c}__min"],
                None,
                "INFO",
                "INFO",
            )
            add(
                "numeric",
                f"max:{c}",
                "max",
                stat_row[f"{c}__max"],
                None,
                "INFO",
                "INFO",
            )
            add(
                "numeric",
                f"zero_count:{c}",
                "count",
                float(stat_row[f"{c}__zero"] or 0),
                None,
                "INFO",
                "INFO",
            )
            add(
                "numeric",
                f"negative_count:{c}",
                "count",
                float(stat_row[f"{c}__neg"] or 0),
                None,
                "INFO",
                "INFO",
            )

    # DQ / semantic-status flag counts (recognised by name, not forced on tables
    # that carry none)
    flag_cols = [c for c in cols if _FLAG_COL_PATTERN.search(c)]
    bool_flag_cols = [c for c in flag_cols if dtypes.get(c) == "boolean"]
    if bool_flag_cols:
        flag_exprs = [F.sum(F.col(c).cast("int")).alias(c) for c in bool_flag_cols]
        flag_row = df_after.agg(*flag_exprs).first()
        for c in bool_flag_cols:
            add(
                "dq",
                f"flag_true_count:{c}",
                "count",
                float(flag_row[c] or 0),
                None,
                "INFO",
                "INFO",
            )
    for c in flag_cols:
        if dtypes.get(c) == "string":
            vc = df_after.groupBy(c).count().collect()
            details = ", ".join(f"{r[c]}={r['count']}" for r in vc)
            add("dq", f"value_counts:{c}", "count", None, None, "INFO", "INFO", details)

    # Source-specific checks the caller already computed -- never invented here
    if extra_checks:
        for name, value in extra_checks.items():
            numeric_value = (
                value
                if isinstance(value, (int, float)) and not isinstance(value, bool)
                else None
            )
            add(
                "transformation_specific",
                name,
                "value",
                numeric_value,
                None,
                "INFO",
                "INFO",
                str(value),
            )

    _log_rows(rows)
    print(f"INSPECTED {full}: {len(rows)} check row(s) -> {INSPECTION_TABLE}")


# COMMAND ----------


# DBTITLE 1,Convenience: reconciliation against a known baseline
def compare_to_baseline(
    silver_table: str,
    check_name: str,
    observed: float,
    expected: float,
    *,
    source: str,
    component: str,
    rid: str,
    tolerance: float = 0.0,
) -> None:
    """Log one row comparing an observed value against a known profiling-era
    baseline (e.g. GA4's 779,485-row staging count, REES46's 102 bot-burst
    sessions). Use for the per-notebook baseline checks listed in the design
    record -- never invents a baseline, the caller supplies it."""
    ts = now_utc()
    full = f"{CATALOG}.{target_schema_for(silver_table, source=source)}.{silver_table}"
    diff = observed - expected
    status = "PASS" if abs(diff) <= tolerance else "WARN"
    _log_rows(
        [
            (
                ts,
                rid,
                source,
                component,
                full,
                "baseline_reconciliation",
                check_name,
                "value",
                float(observed),
                float(expected),
                status,
                "WARN" if status == "WARN" else "INFO",
                f"tolerance={tolerance}",
            )
        ]
    )
    print(
        f"BASELINE  {check_name}: observed={observed} expected={expected} -> {status}"
    )
