# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # ANALYTICS SHARED LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** the reusable Analytics-transform plumbing pulled into every
# MAGIC Analytics notebook with `%run ../../_analytics_common` (after
# MAGIC `%run ../../../gold/_gold_common`, which this file also loads for
# MAGIC `read_gold` / `read_shared_conformed` / `ecosystem_for`). Definitions
# MAGIC only -- no side effects at import; the caller owns `spark`.
# MAGIC
# MAGIC Analytics = large-scale analytical computation and domain marts over
# MAGIC Gold: deliberate grain changes (roll-ups), justified cross-source joins
# MAGIC through conformed keys only, never a fabricated identity. Same hard-fail
# MAGIC gate discipline as Gold's own Silver->Gold gate.

# COMMAND ----------

# DBTITLE 1,Gold shared library (read_gold, add_gold_provenance, CATALOG, ...)
# MAGIC %run ../gold/_gold_common

# COMMAND ----------

# DBTITLE 1,Imports
import datetime as _dt

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration constants
ANALYTICS_STAGE = "analytics"


def analytics_schema_for(source: str) -> str:
    """source_system -> its `{ecosystem}_analytics` schema."""
    return f"{ecosystem_for(source)}_analytics"


def semantic_schema_for(source: str) -> str:
    """source_system -> its `{ecosystem}_semantic` schema."""
    return f"{ecosystem_for(source)}_semantic"


# COMMAND ----------

# DBTITLE 1,Run identity


def analytics_run_id() -> str:
    """A single deterministic-per-execution run id -- same shape as Gold's
    gold_run_id(), distinct prefix so a run_id alone tells you which stage
    produced it."""
    return _dt.datetime.now(_dt.UTC).strftime("analytics-%Y%m%dT%H%M%SZ")


# COMMAND ----------

# DBTITLE 1,Analytics provenance


def add_analytics_provenance(df: DataFrame, source_system: str, rid: str) -> DataFrame:
    return (
        df.withColumn("source_system", F.lit(source_system))
        .withColumn("ecosystem", F.lit(ecosystem_for(source_system)))
        .withColumn("_analytics_loaded_at", F.current_timestamp())
        .withColumn("_analytics_run_id", F.lit(rid))
    )


# COMMAND ----------

# DBTITLE 1,Hard-fail Gold-to-Analytics gate


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
    """The embedded Gold->Analytics check(hard_fail) gate -- same mechanism as
    Gold's own check(), logged to quality_audit_log with stage='analytics'."""
    status = "PASS" if condition else "FAIL"
    row = spark.createDataFrame(
        [
            (
                rid,
                _dt.datetime.now(_dt.UTC).date(),
                source,
                ANALYTICS_STAGE,
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
            f"ANALYTICS GATE FAILED: {component}.{metric_name} -- "
            f"{detail or 'no detail given'}"
        )


def assert_unique_grain(
    df: DataFrame, key_cols: list[str], *, component: str, source: str, rid: str
) -> None:
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

# DBTITLE 1,Analytics write


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


def write_analytics(
    df: DataFrame,
    table: str,
    *,
    schema: str | None = None,
    source: str,
    component: str,
    rid: str,
) -> int | None:
    """Deterministic full overwrite into `{schema or analytics_schema_for(source)}.
    {table}` -- same shape as Gold's write_gold()."""
    started = now_utc()
    full = f"{CATALOG}.{schema or analytics_schema_for(source)}.{table}"
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
                ANALYTICS_STAGE,
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

# DBTITLE 1,Read an Analytics table (for cross-notebook reads)


def read_analytics(table: str, *, source: str, schema: str | None = None) -> DataFrame:
    return spark.table(f"{CATALOG}.{schema or analytics_schema_for(source)}.{table}")