# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- REF_VOCABULARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `shared_conformed.ref_vocabulary` -- the coded-value catalog
# MAGIC and its hierarchy, carried through from `mastr_code_list`. Grain:
# MAGIC vocabulary x code.

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
SOURCE = "mastr"
COMPONENT = "gold/shared/ref_vocabulary"
TABLE = "ref_vocabulary"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = gold_run_id()

# COMMAND ----------

# DBTITLE 1,Read Silver
mastr_code_list = read_silver("mastr_code_list")

# COMMAND ----------

# DBTITLE 1,Build ref_vocabulary
ref_vocabulary = mastr_code_list.select(
    F.col("catalog_kind").alias("vocabulary"),
    F.col("code_id").alias("code"),
    F.col("parent_id").alias("parent_code"),
    F.col("label"),
    F.col("source_system"),
    F.col("source_dataset"),
    F.col("source_record_id"),
)
ref_vocabulary = add_gold_provenance(ref_vocabulary, SOURCE, rid)

# COMMAND ----------

# DBTITLE 1,Grain gate
assert_unique_grain(
    ref_vocabulary, ["vocabulary", "code"], component=COMPONENT, source=SOURCE, rid=rid
)

# COMMAND ----------

# DBTITLE 1,Write
write_gold(
    ref_vocabulary,
    TABLE,
    schema=SHARED_CONFORMED_SCHEMA,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Inspect + export findings
_blocks = inspect_gold_table(
    ref_vocabulary,
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    schema=SHARED_CONFORMED_SCHEMA,
    key_cols=["vocabulary", "code"],
    df_before=mastr_code_list,
)
write_gold_findings(SOURCE, f"shared__{TABLE}", TABLE, _blocks)
