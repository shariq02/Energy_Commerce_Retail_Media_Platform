# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- GENERATION SUPPORT/AUTHORISATION BRIDGES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Grain:** one row per (`parent_id`, `linked_id`) per bridge.
# MAGIC
# MAGIC **Sources:** `mastr_eeg_support_unit_bridge`, `mastr_kwk_support_unit_bridge`,
# MAGIC `mastr_authorisation_unit_bridge`, `mastr_repowering_eeg_bridge` (Silver,
# MAGIC energy_silver_reference); `dim_generation_unit` (Gold, this run).
# MAGIC
# MAGIC **Serves use case:** any Energy use case needing per-unit EEG/KWK support
# MAGIC or authorisation counts -- aggregate through these bridges before joining
# MAGIC to a fact, never join raw and let it fan out.
# MAGIC
# MAGIC **Purpose:** re-key the four additive Silver bridges against
# MAGIC `dim_generation_unit`'s surrogate key. The first three link a support/
# MAGIC authorisation record to a generation unit (`linked_id` = `unit_id`) and
# MAGIC get a `generation_unit_key` FK. `mastr_repowering_eeg_bridge` links a
# MAGIC repowering record to an EEG support record, not a generation unit --
# MAGIC it is re-keyed but carries no `generation_unit_key`.

# COMMAND ----------

# DBTITLE 1,Shared Silver library
# MAGIC %run ../../../silver/_silver_common

# COMMAND ----------

# DBTITLE 1,Gold shared library
# MAGIC %run ../../_gold_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../_gold_inspect

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "mastr"
COMPONENT = "gold/energy/generation/04_bridge_generation_support"
RID = gold_run_id()

# COMMAND ----------

# DBTITLE 1,Read Silver -- mastr_eeg_support_unit_bridge
_eeg_bridge_silver = read_silver("mastr_eeg_support_unit_bridge")

# COMMAND ----------

# DBTITLE 1,Read Silver -- mastr_kwk_support_unit_bridge
_kwk_bridge_silver = read_silver("mastr_kwk_support_unit_bridge")

# COMMAND ----------

# DBTITLE 1,Read Silver -- mastr_authorisation_unit_bridge
_authorisation_bridge_silver = read_silver("mastr_authorisation_unit_bridge")

# COMMAND ----------

# DBTITLE 1,Read Silver -- mastr_repowering_eeg_bridge
_repowering_bridge_silver = read_silver("mastr_repowering_eeg_bridge")

# COMMAND ----------

# DBTITLE 1,Read Gold -- dim_generation_unit
_gen_unit_keys = read_gold("dim_generation_unit", source="mastr").select(
    F.col("unit_id").alias("_gu_unit_id"),
    F.col("generation_unit_key").alias("_gu_key"),
)

# COMMAND ----------

# DBTITLE 1,Transform -- bridge_eeg_support_unit
bridge_eeg_support_unit = (
    _eeg_bridge_silver.join(
        _gen_unit_keys, _eeg_bridge_silver["linked_id"] == F.col("_gu_unit_id"), "left"
    )
    .withColumn("generation_unit_key", F.col("_gu_key"))
    .drop("_gu_unit_id", "_gu_key")
)
bridge_eeg_support_unit = bridge_eeg_support_unit.withColumn(
    "bridge_eeg_support_unit_key", surrogate_key("parent_id", "linked_id")
)
bridge_eeg_support_unit = add_gold_provenance(bridge_eeg_support_unit, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Write Gold -- bridge_eeg_support_unit
write_gold(
    bridge_eeg_support_unit,
    "bridge_eeg_support_unit",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect bridge_eeg_support_unit + export findings
_findings_blocks = inspect_gold_table(
    bridge_eeg_support_unit,
    "bridge_eeg_support_unit",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["parent_id", "linked_id"],
    df_before=_eeg_bridge_silver,
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__bridge_eeg_support_unit",
    "bridge_eeg_support_unit",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- bridge_kwk_support_unit
bridge_kwk_support_unit = (
    _kwk_bridge_silver.join(
        _gen_unit_keys, _kwk_bridge_silver["linked_id"] == F.col("_gu_unit_id"), "left"
    )
    .withColumn("generation_unit_key", F.col("_gu_key"))
    .drop("_gu_unit_id", "_gu_key")
)
bridge_kwk_support_unit = bridge_kwk_support_unit.withColumn(
    "bridge_kwk_support_unit_key", surrogate_key("parent_id", "linked_id")
)
bridge_kwk_support_unit = add_gold_provenance(bridge_kwk_support_unit, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Write Gold -- bridge_kwk_support_unit
write_gold(
    bridge_kwk_support_unit,
    "bridge_kwk_support_unit",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect bridge_kwk_support_unit + export findings
_findings_blocks = inspect_gold_table(
    bridge_kwk_support_unit,
    "bridge_kwk_support_unit",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["parent_id", "linked_id"],
    df_before=_kwk_bridge_silver,
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__bridge_kwk_support_unit",
    "bridge_kwk_support_unit",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- bridge_authorisation_unit
bridge_authorisation_unit = (
    _authorisation_bridge_silver.join(
        _gen_unit_keys,
        _authorisation_bridge_silver["linked_id"] == F.col("_gu_unit_id"),
        "left",
    )
    .withColumn("generation_unit_key", F.col("_gu_key"))
    .drop("_gu_unit_id", "_gu_key")
)
bridge_authorisation_unit = bridge_authorisation_unit.withColumn(
    "bridge_authorisation_unit_key", surrogate_key("parent_id", "linked_id")
)
bridge_authorisation_unit = add_gold_provenance(bridge_authorisation_unit, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Write Gold -- bridge_authorisation_unit
write_gold(
    bridge_authorisation_unit,
    "bridge_authorisation_unit",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect bridge_authorisation_unit + export findings
_findings_blocks = inspect_gold_table(
    bridge_authorisation_unit,
    "bridge_authorisation_unit",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["parent_id", "linked_id"],
    df_before=_authorisation_bridge_silver,
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__bridge_authorisation_unit",
    "bridge_authorisation_unit",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- bridge_repowering_eeg (no generation_unit_key -- links to an EEG record, not a unit)
bridge_repowering_eeg = _repowering_bridge_silver.withColumn(
    "bridge_repowering_eeg_key", surrogate_key("parent_id", "linked_id")
)
bridge_repowering_eeg = add_gold_provenance(bridge_repowering_eeg, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Write Gold -- bridge_repowering_eeg
write_gold(
    bridge_repowering_eeg,
    "bridge_repowering_eeg",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect bridge_repowering_eeg + export findings
_findings_blocks = inspect_gold_table(
    bridge_repowering_eeg,
    "bridge_repowering_eeg",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["parent_id", "linked_id"],
    df_before=_repowering_bridge_silver,
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__bridge_repowering_eeg",
    "bridge_repowering_eeg",
    _findings_blocks,
)
