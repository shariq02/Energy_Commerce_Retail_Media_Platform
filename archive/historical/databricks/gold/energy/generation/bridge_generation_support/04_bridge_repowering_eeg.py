# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- BRIDGE_REPOWERING_EEG
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Grain:** one row per (`parent_id`, `linked_id`).
# MAGIC
# MAGIC **Sources:** `mastr_repowering_eeg_bridge` (Silver, energy_silver_reference); `dim_generation_unit`
# MAGIC (Gold, `../../01_dim_generation_unit.py`).
# MAGIC
# MAGIC **Serves use case:** any Energy use case needing per-unit support/
# MAGIC authorisation counts -- aggregate through this bridge before joining to
# MAGIC a fact, never join raw and let it fan out. One of 4 sibling notebooks in
# MAGIC this folder, split from a single `04_bridge_generation_support.py` --
# MAGIC see the folder's other files for the rest.
# MAGIC
# MAGIC **Purpose:** re-key `mastr_repowering_eeg_bridge`. Unlike the other 3 bridges in this folder, `linked_id` here is an EEG support record id, not a generation unit -- no `generation_unit_key` FK is resolved.

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

# DBTITLE 1,Imports

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "mastr"
COMPONENT = "gold/energy/generation/bridge_generation_support/04_bridge_repowering_eeg"
RID = gold_run_id()
GOLD_TABLE = "bridge_repowering_eeg"

# COMMAND ----------

# DBTITLE 1,Read Silver -- mastr_repowering_eeg_bridge
_bridge_silver = read_silver("mastr_repowering_eeg_bridge")

# COMMAND ----------

# DBTITLE 1,Transform -- bridge_repowering_eeg
bridge = _bridge_silver.withColumn(
    "bridge_repowering_eeg_key", surrogate_key("parent_id", "linked_id")
)
bridge = add_gold_provenance(bridge, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- one row per (parent_id, linked_id)
assert_unique_grain(
    bridge, ["parent_id", "linked_id"], component=COMPONENT, source=SOURCE, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- bridge_repowering_eeg
write_gold(bridge, GOLD_TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect bridge_repowering_eeg + export findings
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
    "bridge_generation_support__bridge_repowering_eeg",
    GOLD_TABLE,
    _findings_blocks,
)
