# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # REDISPATCH MATCH FEATURES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** attributes of each affected-asset text of the redispatch events for the
# MAGIC asset matching tier: text shape and event summary. The matched name and
# MAGIC its confidence are targets, not features.

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
COMPONENT = "ml/energy/features/redispatch_match_features"
TABLE = "features_redispatch_match"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Read the events
events = read_gold("grid_intervention_event", source=SOURCE).filter(
    F.col("affected_asset_text").isNotNull()
)

# COMMAND ----------

# DBTITLE 1,Summarise per affected-asset text
out = (
    events.groupBy("affected_asset_text")
    .agg(
        F.count("*").alias("event_count"),
        F.min(F.to_date("event_start_project")).alias("first_event_date"),
        F.max(F.to_date("event_start_project")).alias("last_event_date"),
        F.max("max_power_mw").alias("max_power_mw"),
        F.first("primary_energy_type", ignorenulls=True).alias("primary_energy_type"),
        F.countDistinct("direction").alias("direction_count"),
    )
    .withColumn("text_length", F.length("affected_asset_text"))
    .withColumn("token_count", F.size(F.split(F.trim("affected_asset_text"), r"\s+")))
    .withColumn("has_digit", F.col("affected_asset_text").rlike(r"[0-9]"))
)
out = add_ml_provenance(out, TABLE, ECO, rid)

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
write_ml_findings(ECO, "features__" + TABLE, TABLE, _blocks)
