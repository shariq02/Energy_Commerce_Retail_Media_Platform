# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # REDISPATCH MATCH TARGET
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** confidence tier of the existing name match between an affected-asset text
# MAGIC and a registered unit. The tier is rule-derived; there is no supervised
# MAGIC label.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../../_ml_common

# COMMAND ----------

# DBTITLE 1,ML inspection library
# MAGIC %run ../../_ml_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "energy"
SOURCE = "redispatch"
COMPONENT = "ml/energy/targets/redispatch_match"
TABLE = "target_redispatch_match"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Read the events
candidates = read_gold("grid_intervention_event", source=SOURCE).filter(
    F.col("affected_asset_text").isNotNull()
)

# COMMAND ----------

# DBTITLE 1,Tier per affected-asset text
out = (
    candidates.groupBy("affected_asset_text")
    .agg(
        F.first("affected_unit_match_name", ignorenulls=True).alias(
            "matched_unit_name"
        ),
        F.first("affected_unit_match_confidence", ignorenulls=True).alias(
            "match_confidence"
        ),
    )
    .withColumn(
        "target_match_tier", F.coalesce(F.col("match_confidence"), F.lit("unmatched"))
    )
    .withColumn("provenance_tier", F.lit("rule_derived"))
)
out = add_ml_provenance(out, TABLE, ECO, rid)

# COMMAND ----------

# DBTITLE 1,Record the dropped rows
_dropped = candidates.select("affected_asset_text").distinct().count() - out.count()
drop_block = ("dropped_rows", markdown_table(["rows_dropped"], [(_dropped,)]))
print(f"rows dropped as invalid: {_dropped}")

# COMMAND ----------

# DBTITLE 1,Grain and forbidden-column gate
_GRAIN = ["affected_asset_text"]
assert_unique_grain(out, _GRAIN, component=COMPONENT, source=SOURCE, rid=rid)
assert_no_forbidden_columns(out, component=COMPONENT, source=SOURCE, rid=rid)
check(
    COMPONENT,
    SOURCE,
    "non_empty",
    out.limit(1).count() > 0,
    detail="no rows produced; check the input filters",
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Write
write_ml(out, TABLE, ecosystem=ECO, source=SOURCE, component=COMPONENT, rid=rid)

# COMMAND ----------

# DBTITLE 1,Inspect and export findings
_blocks = inspect_ml_table(
    read_ml(TABLE, ecosystem=ECO), TABLE, ecosystem=ECO, key_cols=_GRAIN
)
write_ml_findings(ECO, "targets__" + TABLE, TABLE, _blocks)

# COMMAND ----------

# DBTITLE 1,Add the drop count to the findings
write_ml_findings(ECO, "targets__" + TABLE + "__drops", TABLE + " drops", [drop_block])
