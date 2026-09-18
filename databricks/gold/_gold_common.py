# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD SHARED LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** the reusable Gold-transform plumbing pulled into every Gold
# MAGIC notebook with `%run ../../_gold_common` (after `%run ../../_silver_common`,
# MAGIC which this file also loads for `read_silver` / `sha_key` / `ecosystem_for`).
# MAGIC Definitions only -- no side effects at import; the caller owns `spark`.
# MAGIC
# MAGIC Gold = the governed domain model per ecosystem -- dimensions, facts,
# MAGIC additive bridges, conformed integration via `shared_conformed`. Unlike
# MAGIC Silver's non-blocking field-class registry, the Silver-to-Gold gate is
# MAGIC hard-fail: `check()` raises on a failed condition. No `*_ml_features`
# MAGIC tables, no target columns, no model outputs written back to Gold.

# COMMAND ----------

# DBTITLE 1,Shared Silver library (read_silver, sha_key, ecosystem_for, ...)
# MAGIC %run ../silver/_silver_common

# COMMAND ----------

# DBTITLE 1,Imports
import datetime as _dt

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration constants
GOLD_STAGE = "gold"

# The two Gold schema families: one per ecosystem, plus the cross-ecosystem
# conformed schema created separately by
# databricks/setup/03_create_shared_conformed.py.
SHARED_CONFORMED_SCHEMA = "shared_conformed"


def gold_schema_for(source: str) -> str:
    """source_system -> its `{ecosystem}_gold` schema. Gold has no per-table
    registry like Silver's field_class_registry -- one schema per ecosystem is
    the whole rule."""
    return f"{ecosystem_for(source)}_gold"


# COMMAND ----------

# DBTITLE 1,Run identity


def gold_run_id() -> str:
    """A single deterministic-per-execution run id, timestamp-based. Distinct
    from Silver's run_id() (different prefix) so a run_id alone tells you
    which stage produced it."""
    return _dt.datetime.now(_dt.UTC).strftime("gold-%Y%m%dT%H%M%SZ")


# COMMAND ----------

# DBTITLE 1,Deterministic surrogate keys


def surrogate_key(*cols):
    """Column expr: a deterministic Gold surrogate key -- the same mechanism as
    Silver's sha_key, reused rather than reinvented so a key built from the same
    inputs at either layer is identical."""
    return sha_key(*cols)


# COMMAND ----------

# DBTITLE 1,Gold provenance


def add_gold_provenance(df: DataFrame, source_system: str, rid: str) -> DataFrame:
    """Attach the fixed Gold identity/provenance columns. Unlike Silver's
    add_provenance, this never renames a column to source_record_id -- a Gold
    table's own surrogate key column is named by the caller (e.g.
    `generation_unit_key`), not forced to one fixed name."""
    return (
        df.withColumn("source_system", F.lit(source_system))
        .withColumn("ecosystem", F.lit(ecosystem_for(source_system)))
        .withColumn("_gold_loaded_at", F.current_timestamp())
        .withColumn("_gold_run_id", F.lit(rid))
    )


# COMMAND ----------

# DBTITLE 1,Hard-fail Silver-to-Gold gate


def check(
    component: str,
    source: str,
    metric_name: str,
    condition: bool,
    *,
    detail: str = "",
    metric_value: float | None = None,
    rid: str,
) -> None:
    """The embedded Silver->Gold check(hard_fail) gate. Always logs to
    quality_audit_log (stage='gold'); RAISES when `condition` is False -- unlike
    Silver's audit(), a failed Gold check stops the notebook. Use for grain
    assertions, referential-integrity thresholds, and any other Gold-specific
    invariant the design calls for."""
    status = "PASS" if condition else "FAIL"
    row = spark.createDataFrame(
        [
            (
                rid,
                _dt.datetime.now(_dt.UTC).date(),
                source,
                GOLD_STAGE,
                component,
                metric_name,
                metric_value,
                status,
                detail or None,
                _dt.datetime.now(_dt.UTC),
            )
        ],
        "run_id string, run_date date, source string, stage string, component string, "
        "metric_name string, metric_value double, status string, error_detail string, "
        "recorded_at timestamp",
    )
    row.write.format("delta").mode("append").saveAsTable(AUDIT_TABLE)
    if not condition:
        raise RuntimeError(
            f"GOLD GATE FAILED: {component}.{metric_name} -- {detail or 'no detail given'}"
        )


def assert_unique_grain(
    df: DataFrame, key_cols: list[str], *, component: str, source: str, rid: str
) -> None:
    """Hard-fail grain assertion -- the grain a Gold table's own header states
    must actually hold. Raises via check() if any key_cols group has >1 row."""
    dup_n = df.groupBy(*key_cols).count().filter(F.col("count") > 1).count()
    check(
        component,
        source,
        "grain_duplicate_keys",
        dup_n == 0,
        detail=f"key_cols={key_cols} duplicate_groups={dup_n}",
        metric_value=float(dup_n),
        rid=rid,
    )


# COMMAND ----------

# DBTITLE 1,SCD window repair + overlap guard (for a pit_join() dimension)


def close_scd_gaps(
    df: DataFrame,
    key_col: str,
    *,
    valid_from_col: str = "valid_from",
    valid_to_col: str = "valid_to",
) -> DataFrame:
    """Coalesce a NULL/open valid_to with the same key's next valid_from --
    a source that leaves more than one row "still open" (blank end-date) per
    key otherwise leaves every one of those rows open at once, so pit_join()
    matches all of them for any fact timestamp from that point on. Only fills
    a missing valid_to; an explicit one from the source is left as-is (a bad
    explicit value is caught by assert_no_overlapping_windows, not silently
    overwritten here)."""
    w = Window.partitionBy(key_col).orderBy(valid_from_col)
    return df.withColumn(
        valid_to_col,
        F.coalesce(F.col(valid_to_col), F.lead(valid_from_col).over(w)),
    )


def cap_scd_overlaps(
    df: DataFrame,
    key_col: str,
    *,
    valid_from_col: str = "valid_from",
    valid_to_col: str = "valid_to",
    capped_flag_col: str = "_scd_overlap_capped",
) -> DataFrame:
    """Cap each row's valid_to at the next row's valid_from, for the same
    key, whenever a stale explicit valid_to runs past it -- close_scd_gaps()
    only fills a NULL valid_to, not this case. One-sided: a valid_to that
    already ends at or before the next valid_from (a genuine gap) is left
    untouched. Adds `capped_flag_col` for reporting; run before
    assert_no_overlapping_windows(), after close_scd_gaps()."""
    w = Window.partitionBy(key_col).orderBy(valid_from_col)
    next_start = F.lead(valid_from_col).over(w)
    needs_cap = (
        next_start.isNotNull()
        & F.col(valid_to_col).isNotNull()
        & (F.col(valid_to_col) > next_start)
    )
    return df.withColumn(capped_flag_col, needs_cap).withColumn(
        valid_to_col, F.when(needs_cap, next_start).otherwise(F.col(valid_to_col))
    )


def assert_no_overlapping_windows(
    df: DataFrame,
    key_col: str,
    *,
    valid_from_col: str = "valid_from",
    valid_to_col: str = "valid_to",
    component: str,
    source: str,
    rid: str,
) -> None:
    """Hard-fail if any key has two windows covering the same instant -- the
    invariant pit_join() needs to ever resolve to at most one dim row per
    fact. Run this after close_scd_gaps(), not instead of it."""
    w = Window.partitionBy(key_col).orderBy(valid_from_col)
    chk = df.withColumn("_next_valid_from", F.lead(valid_from_col).over(w))
    overlap_n = chk.filter(
        F.col("_next_valid_from").isNotNull()
        & F.col(valid_to_col).isNotNull()
        & (F.col(valid_to_col) > F.col("_next_valid_from"))
    ).count()
    check(
        component,
        source,
        "overlapping_scd_windows",
        overlap_n == 0,
        detail=f"key_col={key_col} overlapping_window_count={overlap_n}",
        metric_value=float(overlap_n),
        rid=rid,
    )


# COMMAND ----------

# DBTITLE 1,Point-in-time-correct dimension join


def pit_join(
    fact_df: DataFrame,
    dim_df: DataFrame,
    *,
    fact_key_col: str,
    fact_ts_col: str,
    dim_key_col: str,
    dim_valid_from_col: str = "valid_from",
    dim_valid_to_col: str = "valid_to",
    dim_select: list[str] | None = None,
) -> DataFrame:
    """Join `dim_df` (a time-varying/SCD dimension) onto `fact_df` on the
    dimension row whose validity window covers the fact's own timestamp --
    never a plain equi-join on the natural key alone, which silently resolves
    to whichever row the join happens to pick. `dim_valid_to_col` NULL means
    "still current"."""
    d = dim_df if dim_select is None else dim_df.select(*dim_select)
    d = d.withColumnRenamed(dim_key_col, "_pit_dim_key")
    cond = (
        (fact_df[fact_key_col] == d["_pit_dim_key"])
        & (
            F.col(dim_valid_from_col).isNull()
            | (F.col(dim_valid_from_col) <= fact_df[fact_ts_col])
        )
        & (
            F.col(dim_valid_to_col).isNull()
            | (F.col(dim_valid_to_col) > fact_df[fact_ts_col])
        )
    )
    return fact_df.join(d, cond, "left").drop("_pit_dim_key")


# COMMAND ----------

# DBTITLE 1,Generic FK resolution


def resolve_fk(
    fact_df: DataFrame,
    dim_df: DataFrame,
    *,
    fact_key_cols: list[str],
    dim_key_cols: list[str],
    dim_surrogate_col: str,
    output_col: str,
) -> DataFrame:
    """Left-join `dim_df`'s surrogate key onto `fact_df` on a natural key
    (single- or multi-column) and add it as `output_col` -- the
    join+alias+drop-temp-columns pattern repeated across most Gold fact/
    bridge notebooks (dim_power_plant's MaStR reconciliation,
    fact_redispatch_measure, the location/actor/generation-unit bridges,
    fact_ecommerce_item's parent-event link, fact_search_visibility's
    repository link, ...). `fact_key_cols` and `dim_key_cols` must be the
    same length and in matching order; an unmatched fact row gets NULL in
    `output_col`, never a fabricated identity."""
    tmp_names = [f"_rfk_{i}" for i in range(len(dim_key_cols))]
    dim_slim = dim_df.select(
        *[F.col(dc).alias(tn) for dc, tn in zip(dim_key_cols, tmp_names)],
        F.col(dim_surrogate_col).alias("_rfk_key"),
    )
    cond = None
    for fc, tn in zip(fact_key_cols, tmp_names):
        c = fact_df[fc] == dim_slim[tn]
        cond = c if cond is None else (cond & c)
    out = fact_df.join(dim_slim, cond, "left").withColumn(output_col, F.col("_rfk_key"))
    return out.drop(*tmp_names, "_rfk_key")


# COMMAND ----------

# DBTITLE 1,Aggregate-before-join helper for M:N bridges


def aggregate_bridge(
    bridge_df: DataFrame,
    group_col: str,
    *,
    count_alias: str = "linked_count",
) -> DataFrame:
    """Pre-aggregate an additive M:N bridge to one row per `group_col` before
    it is joined onto a fact/dimension -- the mechanism that keeps every Gold
    join from fanning a fact out across a bridge's linked rows. Callers needing
    more than a count build their own `.groupBy(group_col).agg(...)`; this
    covers the common case."""
    return bridge_df.groupBy(group_col).agg(
        F.count(F.lit(1)).alias(count_alias),
    )


# COMMAND ----------

# DBTITLE 1,Gold write


def _delta_rows_written(table: str) -> int | None:
    try:
        m = (
            spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1")
            .select("operationMetrics")
            .first()[0]
        )
        return int(m.get("numOutputRows")) if m and "numOutputRows" in m else None
    except Exception:
        return None


def write_gold(
    df: DataFrame,
    gold_table: str,
    *,
    schema: str | None = None,
    source: str,
    component: str,
    rid: str,
) -> int | None:
    """Deterministic full overwrite into `{schema or gold_schema_for(source)}.
    {gold_table}`. Prints the Delta rollback line, records rows_written from
    Delta metrics (no pre-write count), writes a watermark row (stage='gold')."""
    started = now_utc()
    full = f"{CATALOG}.{schema or gold_schema_for(source)}.{gold_table}"
    if spark.catalog.tableExists(full):
        v = spark.sql(f"DESCRIBE HISTORY {full} LIMIT 1").first()["version"]
        print(f"ROLLBACK IF NEEDED: RESTORE TABLE {full} TO VERSION AS OF {v}")
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full)
    )
    n = _delta_rows_written(full)
    row = spark.createDataFrame(
        [
            (
                rid,
                source,
                GOLD_STAGE,
                component,
                None,
                None,
                None if n is None else int(n),
                "COMPLETE",
                started,
                _dt.datetime.now(_dt.UTC),
            )
        ],
        "run_id string, source string, stage string, component string, "
        "last_processed_ts timestamp, last_processed_row bigint, rows_written bigint, "
        "status string, started_at timestamp, completed_at timestamp",
    )
    row.write.format("delta").mode("append").saveAsTable(WATERMARK_TABLE)
    print(f"OK  {full}: {n if n is not None else '?'} rows")
    return n


# COMMAND ----------

# DBTITLE 1,Read a Gold table (for cross-notebook / cross-domain reads)


def read_gold(table: str, *, source: str, schema: str | None = None) -> DataFrame:
    return spark.table(f"{CATALOG}.{schema or gold_schema_for(source)}.{table}")


def read_shared_conformed(table: str) -> DataFrame:
    return spark.table(f"{CATALOG}.{SHARED_CONFORMED_SCHEMA}.{table}")
