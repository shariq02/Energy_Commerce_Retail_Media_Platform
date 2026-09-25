# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- REGISTER LINK (MASTR)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** every MaStR link array as one typed relationship structure:
# MAGIC one row per (relationship_type, parent_id, linked_id), with the parent and
# MAGIC linked structure named on each row.

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
COMPONENT = "silver/energy/generation/07_register_link"
RID = run_id()
FINDINGS = "mastr"
TABLE = "register_link"
_EEG = [
    "mastr_anlagen_eeg_wind",
    "mastr_anlagen_eeg_biomasse",
    "mastr_anlagen_eeg_wasser",
    "mastr_anlagen_eeg_geothermie_gsgk",
]
_UNITS = "VerknuepfteEinheitenMaStRNummern"
# relationship_type -> (parent_type, linked_type, Bronze tables, parent col, link col)
LINKS = {
    "eeg_support_unit": (
        "support_registration",
        "generation_unit",
        _EEG,
        "EegMaStRNummer",
        _UNITS,
    ),
    "kwk_support_unit": (
        "support_registration",
        "generation_unit",
        ["mastr_anlagen_kwk"],
        "KwkMastrNummer",
        _UNITS,
    ),
    "authorisation_unit": (
        "unit_authorisation",
        "generation_unit",
        ["mastr_einheiten_genehmigung"],
        "GenMastrNummer",
        _UNITS,
    ),
    "repowering_eeg": (
        "unit_repowering",
        "support_registration",
        ["mastr_ertuechtigungen"],
        "Id",
        "EegMastrNummer",
    ),
    "location_unit": (
        "grid_location",
        "generation_unit",
        ["mastr_lokationen"],
        "MastrNummer",
        _UNITS,
    ),
    "location_connection": (
        "grid_location",
        "grid_connection_point",
        ["mastr_lokationen"],
        "MastrNummer",
        "NetzanschlusspunkteMaStRNummern",
    ),
}

# COMMAND ----------

# DBTITLE 1,Read Bronze -- every table carrying a link column
_tables = {t for _, _, tables, _, _ in LINKS.values() for t in tables}
bronze = {t: read_bronze(t) for t in [*_tables, "mastr_marktakteure_und_rollen"]}

# COMMAND ----------

# DBTITLE 1,Transform -- delimited link arrays to typed pairs
links = None
for rel, (parent_type, linked_type, tables, parent_col, link_col) in LINKS.items():
    for t in tables:
        part = (
            link_pairs(bronze[t], parent_col, link_col)
            .withColumn("relationship_type", F.lit(rel))
            .withColumn("parent_type", F.lit(parent_type))
            .withColumn("linked_type", F.lit(linked_type))
            .withColumn("source_dataset", F.lit(t))
        )
        links = part if links is None else links.unionByName(part)

# COMMAND ----------

# DBTITLE 1,Read Silver -- market role labels
roles = (
    read_silver("mastr_code_list")
    .filter(F.col("catalog_kind") == "marktrollen")
    .select(F.trim("label").alias("_role_label"), F.col("code_id").alias("_role_id"))
)

# COMMAND ----------

# DBTITLE 1,Transform -- actor -> market role code (label resolved to its id)
actor_roles = (
    bronze["mastr_marktakteure_und_rollen"]
    .select(
        F.col("MarktakteurMastrNummer").cast("string").alias("parent_id"),
        F.trim(F.col("Marktrolle").cast("string")).alias("_label"),
    )
    .dropna()
    .dropDuplicates()
    .join(F.broadcast(roles), F.col("_label") == F.col("_role_label"), "left")
    .withColumn("linked_id", F.coalesce(F.col("_role_id"), F.col("_label")))
    .drop("_label", "_role_label", "_role_id")
    .withColumn("relationship_type", F.lit("actor_role"))
    .withColumn("parent_type", F.lit("market_actor"))
    .withColumn("linked_type", F.lit("mastr_code_list"))
    .withColumn("source_dataset", F.lit("mastr_marktakteure_und_rollen"))
)
links = links.unionByName(actor_roles).dropDuplicates(
    ["relationship_type", "parent_id", "linked_id"]
)
_link_key = ["relationship_type", "parent_id", "linked_id"]
links = links.withColumn("_srid", sha_key(*_link_key))
links = add_semantic_provenance(links, SOURCE, None, RID, "_srid")

# COMMAND ----------

# DBTITLE 1,Write Silver -- register_link
write_semantic(
    conform(links, SEMANTIC_STRUCTURES[TABLE]),
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect -- register_link
written = spark.table(semantic_table(TABLE))
findings_blocks = inspect_table(
    written,
    TABLE,
    source=FINDINGS,
    component=COMPONENT,
    rid=RID,
    key_cols=["relationship_type", "parent_id", "linked_id"],
    extra_checks={
        "rows_by_relationship_type": {
            r["relationship_type"]: r["count"]
            for r in written.groupBy("relationship_type").count().collect()
        }
    },
)

# COMMAND ----------

# DBTITLE 1,Export findings -- register_link
write_silver_findings(
    FINDINGS, f"{COMPONENT.split('/')[-1]}__{TABLE}", TABLE, findings_blocks
)
