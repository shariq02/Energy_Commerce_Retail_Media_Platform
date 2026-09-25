# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- REGISTER CHANGE EVENTS (MASTR)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** unit deletions, actor deletions and grid-operator changes
# MAGIC as three event structures (different targets and time meaning), with
# MAGIC additive date-order flags on the grid-operator changes.

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
COMPONENT = "silver/energy/grid/08_register_change_event"
RID = run_id()
FINDINGS = "mastr"
MAPPING = load_mapping(SOURCE)
NAME_MAP = flatten_business_names(MAPPING, SOURCE)
CODED = coded_columns(MAPPING, SOURCE)
DELETIONS = {
    "unit_deletion_event": ("mastr_geloeschte_deaktivierte_einheiten", "unit_id"),
    "actor_deletion_event": (
        "mastr_geloeschte_deaktivierte_marktakteure",
        "market_actor_id",
    ),
}
CHANGE_BT = "mastr_einheiten_aenderung_netzbetreiberzuordnungen"
CHANGE_KEY = ["unit_id", "grid_operator_change_effective_date"]
KEYS = {
    "grid_operator_change_event": [
        *CHANGE_KEY,
        "grid_operator_change_registered_date",
        "change_type",
        "previous_grid_operator_id",
        "new_grid_operator_id",
    ]
}

# COMMAND ----------

# DBTITLE 1,Read Bronze -- unit and actor deletions
deletion_bronze = {name: read_bronze(bt) for name, (bt, _) in DELETIONS.items()}

# COMMAND ----------

# DBTITLE 1,Read Bronze -- grid-operator changes
change_bronze = read_bronze(CHANGE_BT)

# COMMAND ----------

# DBTITLE 1,Transform -- unit_deletion_event and actor_deletion_event
STRUCTURES = {}
for name, (bt, id_col) in DELETIONS.items():
    df = mastr_standardise(deletion_bronze[name], NAME_MAP, CODED)
    df = df.withColumn("_srid", F.col(id_col).cast("string"))
    STRUCTURES[name] = add_semantic_provenance(df, SOURCE, bt, RID, "_srid")

# COMMAND ----------

# DBTITLE 1,Read Silver -- generation_unit commissioning dates
commissioning = (
    spark.table(semantic_table("generation_unit"))
    .filter(F.col("commissioning_date").isNotNull())
    .select("unit_id", "commissioning_date")
)

# COMMAND ----------

# DBTITLE 1,Transform -- grid_operator_change_event (key not unique: ordinal)
changes = mastr_standardise(change_bronze, NAME_MAP, CODED)
changes = within_group_ordinal(
    changes, CHANGE_KEY, [c for c in changes.columns if c not in CHANGE_KEY]
)
_registered = F.col("grid_operator_change_registered_date")
_effective = F.col("grid_operator_change_effective_date")
changes = (
    changes.join(F.broadcast(commissioning), "unit_id", "left")
    .withColumn(
        "_date_order_violation_registered_before_effective",
        _registered < _effective,
    )
    .withColumn(
        "_date_order_violation_commissioning_after_change",
        F.col("commissioning_date") > _effective,
    )
    .drop("commissioning_date")
    .withColumn("_srid", sha_key(*CHANGE_KEY, "_src_id_ord"))
)
STRUCTURES["grid_operator_change_event"] = add_semantic_provenance(
    changes, SOURCE, CHANGE_BT, RID, "_srid"
)

# COMMAND ----------

# DBTITLE 1,Write Silver -- change event structures
for name, frame in STRUCTURES.items():
    write_semantic(frame, name, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect -- change event structures
findings_blocks = {
    name: inspect_table(
        spark.table(semantic_table(name)),
        name,
        source=FINDINGS,
        component=COMPONENT,
        rid=RID,
        key_cols=KEYS.get(name, ["source_record_id"]),
    )
    for name in STRUCTURES
}

# COMMAND ----------

# DBTITLE 1,Export findings -- change event structures
for name, blocks in findings_blocks.items():
    write_silver_findings(FINDINGS, f"{COMPONENT.split('/')[-1]}__{name}", name, blocks)
