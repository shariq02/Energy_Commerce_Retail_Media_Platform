# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- QUALITY_EXCEPTION
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `shared_conformed.quality_exception` -- a view over
# MAGIC `quality.quarantine`, latest quarantine event only per (rule, source,
# MAGIC source record) -- `quarantine` is an append-only log across every Silver
# MAGIC run, so a record re-flagged on a later run would otherwise collide with
# MAGIC its own earlier entry. Grain: rule x source x source record.

# COMMAND ----------

# DBTITLE 1,Shared Silver library
# MAGIC %run ../../silver/_silver_common

# COMMAND ----------

# DBTITLE 1,Gold shared library
# MAGIC %run ../_gold_common

# COMMAND ----------

# DBTITLE 1,Gold inspection library
# MAGIC %run ../_gold_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "quality"
COMPONENT = "gold/shared/quality_exception"
TABLE = "quality_exception"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = gold_run_id()

# COMMAND ----------

# DBTITLE 1,Create the view
write_gold_view(
    f"""
SELECT rule, source, bronze_table, source_record_id, reason, field_name,
       offending_value, run_id, quarantined_at
FROM (
    SELECT
        rule_id           AS rule,
        source_system      AS source,
        bronze_table,
        source_record_id,
        reason,
        field_name,
        offending_value,
        run_id,
        quarantined_at,
        ROW_NUMBER() OVER (
            PARTITION BY rule_id, source_system, source_record_id
            ORDER BY quarantined_at DESC
        ) AS _rn
    FROM {QUARANTINE_TABLE}
)
WHERE _rn = 1
""",
    TABLE,
    schema=SHARED_CONFORMED_SCHEMA,
    source=SOURCE,
)

# COMMAND ----------

# DBTITLE 1,Inspect + export findings
_blocks = inspect_gold_table(
    spark.table(f"{CATALOG}.{SHARED_CONFORMED_SCHEMA}.{TABLE}"),
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    schema=SHARED_CONFORMED_SCHEMA,
    key_cols=["rule", "source", "source_record_id"],
)
write_gold_findings(SOURCE, f"shared__{TABLE}", TABLE, _blocks)
