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

# DBTITLE 1,Inspection library
# MAGIC %run ../../_silver_inspect

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "mastr"
COMPONENT = "silver/energy/grid/03_mastr_change_logs"
RID = run_id()

MAPPING = load_mapping(SOURCE)
NAME_MAP = flatten_business_names(MAPPING, SOURCE)
CODED = coded_columns(MAPPING, SOURCE)

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_geloeschte_deaktivierte_einheiten
_u_bronze = read_bronze("mastr_geloeschte_deaktivierte_einheiten")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_geloeschte_deaktivierte_marktakteure
_a_bronze = read_bronze("mastr_geloeschte_deaktivierte_marktakteure")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_einheiten_aenderung_netzbetreiberzuordnungen (non-unique key -> content-hash ordinal)
_g_bronze = read_bronze("mastr_einheiten_aenderung_netzbetreiberzuordnungen")

# COMMAND ----------

# DBTITLE 1,Transform -- mastr_unit_deletion_events
u = mastr_standardise(_u_bronze, NAME_MAP, CODED, source=SOURCE)
u = u.withColumn("_srid", F.col("unit_id").cast("string"))
u = add_provenance(u, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- mastr_unit_deletion_events
write_silver(
    u, "mastr_unit_deletion_events", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect mastr_unit_deletion_events + export findings
_findings_blocks = inspect_table(
    u,
    "mastr_unit_deletion_events",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["unit_id"],
    df_before=_u_bronze,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__mastr_unit_deletion_events",
    "mastr_unit_deletion_events",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- mastr_actor_deletion_events
a = mastr_standardise(_a_bronze, NAME_MAP, CODED, source=SOURCE)
a = a.withColumn("_srid", F.col("market_actor_id").cast("string"))
a = add_provenance(a, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- mastr_actor_deletion_events
write_silver(
    a, "mastr_actor_deletion_events", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect mastr_actor_deletion_events + export findings
_findings_blocks = inspect_table(
    a,
    "mastr_actor_deletion_events",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["market_actor_id"],
    df_before=_a_bronze,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__mastr_actor_deletion_events",
    "mastr_actor_deletion_events",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform mastr_grid_operator_change_events
g = mastr_standardise(_g_bronze, NAME_MAP, CODED, source=SOURCE)
_key = ["unit_id", "grid_operator_change_effective_date"]
g = within_group_ordinal(g, _key, [c for c in g.columns if c not in _key])
g = g.withColumn("_srid", sha_key(*_key, "_src_id_ord"))

# COMMAND ----------

# DBTITLE 1,Date-order flag -- registered before effective (additive)
# Two date-order flags, additive, never drop/correct rows --
# (1) registration before effective date; (2) commissioning after the change
# (needs 01_mastr_generation_units.py to have run first).
g = g.withColumn(
    "_date_order_violation_registered_before_effective",
    F.when(
        F.col("grid_operator_change_registered_date").isNotNull()
        & F.col("grid_operator_change_effective_date").isNotNull(),
        F.col("grid_operator_change_registered_date")
        < F.col("grid_operator_change_effective_date"),
    ).otherwise(F.lit(False)),
)

# COMMAND ----------

# DBTITLE 1,Date-order flag -- build the commissioning_date lookup
_GENERATION_UNIT_TABLES = [
    "mastr_einheiten_wind",
    "mastr_einheiten_biomasse",
    "mastr_einheiten_wasser",
    "mastr_einheiten_verbrennung",
    "mastr_einheiten_kernkraft",
    "mastr_einheiten_geothermie_gsgk",
]
_commissioning = None
for _t in _GENERATION_UNIT_TABLES:
    try:
        _part = (
            read_silver(_t)
            .filter(F.col("commissioning_date").isNotNull())
            .select("unit_id", "commissioning_date")
        )
    except Exception as exc:
        print(f"SKIP {_t} in commissioning check: {exc}")
        continue
    _commissioning = (
        _part if _commissioning is None else _commissioning.unionByName(_part)
    )

# COMMAND ----------

# DBTITLE 1,Date-order flag -- join + commissioning after change (additive)
if _commissioning is not None:
    g = (
        g.join(F.broadcast(_commissioning), "unit_id", "left")
        .withColumn(
            "_date_order_violation_commissioning_after_change",
            F.when(
                F.col("commissioning_date").isNotNull()
                & F.col("grid_operator_change_effective_date").isNotNull(),
                F.col("commissioning_date")
                > F.col("grid_operator_change_effective_date"),
            ).otherwise(F.lit(False)),
        )
        .drop("commissioning_date")
    )
else:
    g = g.withColumn(
        "_date_order_violation_commissioning_after_change", F.lit(None).cast("boolean")
    )
    print(
        "no generation-unit Silver tables available yet -- commissioning check skipped."
    )

# COMMAND ----------

# DBTITLE 1,Write mastr_grid_operator_change_events -> Silver
g = add_provenance(g, SOURCE, "_srid", RID)
write_silver(
    g, "mastr_grid_operator_change_events", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect mastr_grid_operator_change_events + export findings
_findings_blocks = inspect_table(
    g,
    "mastr_grid_operator_change_events",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=_key,
    df_before=_g_bronze,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__mastr_grid_operator_change_events",
    "mastr_grid_operator_change_events",
    _findings_blocks,
)
