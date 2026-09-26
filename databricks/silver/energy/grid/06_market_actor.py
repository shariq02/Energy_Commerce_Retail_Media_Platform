# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- MARKET ACTOR (MASTR)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** MaStR market actors and their role registrations (separate
# MAGIC identities); the actor -> role link is in `register_link`.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../_silver_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../_silver_inspect

# COMMAND ----------

# DBTITLE 1,Semantic structures library
# MAGIC %run ../../_semantic_common

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "mastr"
COMPONENT = "silver/energy/grid/06_market_actor"
RID = run_id()
FINDINGS = "mastr"
MAPPING = load_mapping(SOURCE)
NAME_MAP = flatten_business_names(MAPPING, SOURCE)
CODED = coded_columns(MAPPING, SOURCE)
# structure -> Bronze table (both keyed by their own MastrNummer)
STRUCTURES_BRONZE = {
    "market_actor": "mastr_marktakteure",
    "market_actor_role": "mastr_marktakteure_und_rollen",
}

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_marktakteure and mastr_marktakteure_und_rollen
bronze = {name: read_bronze(bt) for name, bt in STRUCTURES_BRONZE.items()}

# COMMAND ----------

# DBTITLE 1,Transform -- market_actor and market_actor_role
STRUCTURES = {}
for name, bt in STRUCTURES_BRONZE.items():
    names = {**NAME_MAP, **MASTR_KEY_NAMES.get(bt, {})}
    df = mastr_standardise(bronze[name], names, CODED)
    df = df.withColumn("_srid", F.col(names["MastrNummer"]).cast("string"))
    STRUCTURES[name] = add_semantic_provenance(df, SOURCE, bt, RID, "_srid")

# COMMAND ----------

# DBTITLE 1,Write Silver -- market_actor and market_actor_role
for name, frame in STRUCTURES.items():
    write_semantic(frame, name, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect -- market_actor and market_actor_role
findings_blocks = {
    name: inspect_table(
        spark.table(semantic_table(name)),
        name,
        source=FINDINGS,
        component=COMPONENT,
        rid=RID,
        key_cols=["source_record_id"],
        df_before=bronze[name],
    )
    for name in STRUCTURES
}

# COMMAND ----------

# DBTITLE 1,Export findings -- market_actor and market_actor_role
for name, blocks in findings_blocks.items():
    write_silver_findings(FINDINGS, f"{COMPONENT.split('/')[-1]}__{name}", name, blocks)
