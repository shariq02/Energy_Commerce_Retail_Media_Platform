# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- BRIDGE_EEG_SUPPORT_UNIT
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Grain:** one row per (`parent_id`, `linked_id`).
# MAGIC
# MAGIC **Sources:** `mastr_eeg_support_unit_bridge` (Silver, energy_silver_reference); `dim_generation_unit`
# MAGIC (Gold, `../../01_dim_generation_unit.py`).
# MAGIC
# MAGIC **Serves use case:** any Energy use case needing per-unit support/
# MAGIC authorisation counts -- aggregate through this bridge before joining to
# MAGIC a fact, never join raw and let it fan out. One of 4 sibling notebooks in
# MAGIC this folder, split from a single `04_bridge_generation_support.py` --
# MAGIC see the folder's other files for the rest.
# MAGIC
# MAGIC **Purpose:** re-key `mastr_eeg_support_unit_bridge` against `dim_generation_unit`'s surrogate key -- `linked_id` = `unit_id`, resolved to `generation_unit_key`.

# COMMAND ----------

# DBTITLE 1,Shared Silver library
# MAGIC %run ../../../../silver/_silver_common

# COMMAND ----------

# DBTITLE 1,Gold shared library
# MAGIC %run ../../../_gold_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../../_gold_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "mastr"
COMPONENT = (
    "gold/energy/generation/bridge_generation_support/01_bridge_eeg_support_unit"
)
RID = gold_run_id()
GOLD_TABLE = "bridge_eeg_support_unit"

# COMMAND ----------

# DBTITLE 1,Read Silver -- mastr_eeg_support_unit_bridge
_bridge_silver = read_silver("mastr_eeg_support_unit_bridge")

# COMMAND ----------

# DBTITLE 1,Read Gold -- dim_generation_unit
_gen_unit = read_gold("dim_generation_unit", source="mastr")

# COMMAND ----------

# DBTITLE 1,Transform -- bridge_eeg_support_unit
bridge = resolve_fk(
    _bridge_silver,
    _gen_unit,
    fact_key_cols=["linked_id"],
    dim_key_cols=["unit_id"],
    dim_surrogate_col="generation_unit_key",
    output_col="generation_unit_key",
)
bridge = bridge.withColumn(
    "bridge_eeg_support_unit_key", surrogate_key("parent_id", "linked_id")
)
bridge = add_gold_provenance(bridge, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- one row per (parent_id, linked_id)
assert_unique_grain(
    bridge, ["parent_id", "linked_id"], component=COMPONENT, source=SOURCE, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- bridge_eeg_support_unit
write_gold(bridge, GOLD_TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect bridge_eeg_support_unit + export findings
_findings_blocks = inspect_gold_table(
    bridge,
    GOLD_TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["parent_id", "linked_id"],
    df_before=_bridge_silver,
)
write_gold_findings(
    SOURCE,
    "bridge_generation_support__bridge_eeg_support_unit",
    GOLD_TABLE,
    _findings_blocks,
)
