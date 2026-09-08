# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- MASTR EEG / KWK SUPPORT, AUTHORISATION, REPOWERING
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** the seven MaStR support / authorisation / repowering Bronze
# MAGIC tables into source-scoped Silver at their own record grain, plus four
# MAGIC additive `(parent_id, linked_id)` bridge tables from the delimited
# MAGIC `VerknuepfteEinheitenMaStRNummern` link arrays. The bridges never replace
# MAGIC their owning table. Runs after `02_mastr_reference_catalogs`.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../_silver_common

# COMMAND ----------

# DBTITLE 1,Imports + config
from pyspark.sql import functions as F

SOURCE = "mastr"
COMPONENT = "silver/energy/generation/02_mastr_support_authorisation"
RID = run_id()

MAPPING = load_mapping(SOURCE)
NAME_MAP = flatten_business_names(MAPPING, SOURCE)
CODED = coded_columns(MAPPING, SOURCE)

# Bronze table -> primary-key column (renamed to its business name for the id).
PRIMARY_TABLES = {
    "mastr_anlagen_eeg_wind": "EegMaStRNummer",
    "mastr_anlagen_eeg_biomasse": "EegMaStRNummer",
    "mastr_anlagen_eeg_wasser": "EegMaStRNummer",
    "mastr_anlagen_eeg_geothermie_gsgk": "EegMaStRNummer",
    "mastr_anlagen_kwk": "KwkMastrNummer",
    "mastr_einheiten_genehmigung": "GenMastrNummer",
    "mastr_ertuechtigungen": "Id",
}
EEG_TABLES = [t for t in PRIMARY_TABLES if t.startswith("mastr_anlagen_eeg_")]
LINK_COL = "VerknuepfteEinheitenMaStRNummern"

# COMMAND ----------

# DBTITLE 1,Primary support / authorisation tables -> Silver


def process(bt: str, pk: str) -> None:
    df = mastr_standardise(read_bronze(bt), NAME_MAP, CODED, source=SOURCE)
    id_col = NAME_MAP.get(pk, pk)
    df = df.withColumn("_srid", F.col(id_col).cast("string"))
    df = add_provenance(df, SOURCE, "_srid", RID)
    write_silver(df, bt, source=SOURCE, component=COMPONENT, rid=RID)


for _bt, _pk in PRIMARY_TABLES.items():
    process(_bt, _pk)

# COMMAND ----------

# DBTITLE 1,Additive link bridges


def _link_src(tables: list[str], parent: str) -> "DataFrame":
    parts = [read_bronze(t).select(parent, LINK_COL) for t in tables]
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out


explode_link_bridge(
    _link_src(EEG_TABLES, "EegMaStRNummer"),
    "EegMaStRNummer",
    LINK_COL,
    "mastr_eeg_support_unit_bridge",
    source=SOURCE,
    component=COMPONENT,
    bronze_table=",".join(EEG_TABLES),
    rid=RID,
)
explode_link_bridge(
    read_bronze("mastr_anlagen_kwk"),
    "KwkMastrNummer",
    LINK_COL,
    "mastr_kwk_support_unit_bridge",
    source=SOURCE,
    component=COMPONENT,
    bronze_table="mastr_anlagen_kwk",
    rid=RID,
)
explode_link_bridge(
    read_bronze("mastr_einheiten_genehmigung"),
    "GenMastrNummer",
    LINK_COL,
    "mastr_authorisation_unit_bridge",
    source=SOURCE,
    component=COMPONENT,
    bronze_table="mastr_einheiten_genehmigung",
    rid=RID,
)
explode_link_bridge(
    read_bronze("mastr_ertuechtigungen"),
    "Id",
    "EegMastrNummer",
    "mastr_repowering_eeg_bridge",
    source=SOURCE,
    component=COMPONENT,
    bronze_table="mastr_ertuechtigungen",
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Summary
print("=" * 70)
print(f"MASTR SUPPORT / AUTHORISATION -- COMPLETE  (run_id {RID})")
print("=" * 70)
