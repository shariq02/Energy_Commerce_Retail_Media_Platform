# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- MASTR MARKET ACTORS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `mastr_marktakteure` and `mastr_marktakteure_und_rollen`
# MAGIC into source-scoped Silver at their own grain, plus an additive
# MAGIC `mastr_actor_role_bridge` (one row per actor x role). Natural-person
# MAGIC actors keep their row with name / address suppressed at source. Runs
# MAGIC after `02_mastr_reference_catalogs`.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../_silver_common

# COMMAND ----------

# DBTITLE 1,Imports + config
from pyspark.sql import functions as F

SOURCE = "mastr"
COMPONENT = "silver/energy/grid/01_mastr_market_actors"
RID = run_id()

MAPPING = load_mapping(SOURCE)
NAME_MAP = flatten_business_names(MAPPING, SOURCE)
CODED = coded_columns(MAPPING, SOURCE)

# COMMAND ----------

# DBTITLE 1,mastr_marktakteure -> Silver
act = mastr_standardise(
    read_bronze("mastr_marktakteure"), NAME_MAP, CODED, source=SOURCE
)
act = act.withColumn("_srid", F.col("MastrNummer").cast("string"))
act = add_provenance(act, SOURCE, "_srid", RID)
write_silver(act, "mastr_marktakteure", source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,mastr_marktakteure_und_rollen -> Silver
rol = mastr_standardise(
    read_bronze("mastr_marktakteure_und_rollen"), NAME_MAP, CODED, source=SOURCE
)
rol = rol.withColumn("_srid", F.col("MastrNummer").cast("string"))
rol = add_provenance(rol, SOURCE, "_srid", RID)
write_silver(
    rol, "mastr_marktakteure_und_rollen", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,mastr_actor_role_bridge (additive)
brg = (
    read_bronze("mastr_marktakteure_und_rollen")
    .select(
        F.col("MarktakteurMastrNummer").cast("string").alias("parent_id"),
        F.col("Marktrolle").cast("string").alias("linked_id"),
    )
    .filter(F.col("parent_id").isNotNull() & F.col("linked_id").isNotNull())
    .dropDuplicates(["parent_id", "linked_id"])
    .withColumn("_srid", sha_key("parent_id", "linked_id"))
)
brg = add_provenance(brg, SOURCE, "_srid", RID)
write_silver(
    brg, "mastr_actor_role_bridge", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Summary
print("=" * 70)
print(f"MASTR MARKET ACTORS -- COMPLETE  (run_id {RID})")
print("=" * 70)
