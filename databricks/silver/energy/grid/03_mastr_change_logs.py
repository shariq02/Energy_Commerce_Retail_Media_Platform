# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- MASTR CHANGE LOGs
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** the three MaStR change-history Bronze tables into
# MAGIC append-only source-scoped Silver event tables -- unit deletions, actor
# MAGIC departures and grid-operator reassignments. These carry the survivorship
# MAGIC record the live tables omit and are NEVER merged back into the
# MAGIC current-state dimensions. Runs after `02_mastr_reference_catalogs`.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../_silver_common

# COMMAND ----------

# DBTITLE 1,Imports + config
from pyspark.sql import functions as F

SOURCE = "mastr"
COMPONENT = "silver/energy/grid/03_mastr_change_logs"
RID = run_id()

MAPPING = load_mapping(SOURCE)
NAME_MAP = flatten_business_names(MAPPING, SOURCE)
CODED = coded_columns(MAPPING, SOURCE)

# COMMAND ----------

# DBTITLE 1,mastr_unit_deletion_events
u = mastr_standardise(
    read_bronze("mastr_geloeschte_deaktivierte_einheiten"),
    NAME_MAP,
    CODED,
    source=SOURCE,
)
u = u.withColumn("_srid", F.col("unit_id").cast("string"))
u = add_provenance(u, SOURCE, "_srid", RID)
write_silver(
    u, "mastr_unit_deletion_events", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,mastr_actor_deletion_events
a = mastr_standardise(
    read_bronze("mastr_geloeschte_deaktivierte_marktakteure"),
    NAME_MAP,
    CODED,
    source=SOURCE,
)
a = a.withColumn("_srid", F.col("market_actor_id").cast("string"))
a = add_provenance(a, SOURCE, "_srid", RID)
write_silver(
    a, "mastr_actor_deletion_events", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,mastr_grid_operator_change_events (non-unique key -> content-hash ordinal)
g = mastr_standardise(
    read_bronze("mastr_einheiten_aenderung_netzbetreiberzuordnungen"),
    NAME_MAP,
    CODED,
    source=SOURCE,
)
_key = ["unit_id", "grid_operator_change_effective_date"]
g = within_group_ordinal(g, _key, [c for c in g.columns if c not in _key])
g = g.withColumn("_srid", sha_key(*_key, "_src_id_ord"))
g = add_provenance(g, SOURCE, "_srid", RID)
write_silver(
    g, "mastr_grid_operator_change_events", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Summary
print("=" * 70)
print(f"MASTR CHANGE LOGS -- COMPLETE  (run_id {RID})")
print("=" * 70)
