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
# MAGIC tables into source-scoped Silver at their own record grain, one block
# MAGIC per table (read / transform / write / inspect each their own cell),
# MAGIC plus four additive `(parent_id, linked_id)` bridge tables from the
# MAGIC delimited `VerknuepfteEinheitenMaStRNummern` link arrays. The bridges
# MAGIC never replace their owning table. Runs after `02_mastr_reference_catalogs`.


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
COMPONENT = "silver/energy/generation/02_mastr_support_authorisation"
RID = run_id()

MAPPING = load_mapping(SOURCE)
NAME_MAP = flatten_business_names(MAPPING, SOURCE)
CODED = coded_columns(MAPPING, SOURCE)
LINK_COL = "VerknuepfteEinheitenMaStRNummern"

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_anlagen_eeg_wind
_bronze_anlagen_eeg_wind = read_bronze("mastr_anlagen_eeg_wind")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_anlagen_eeg_biomasse
_bronze_anlagen_eeg_biomasse = read_bronze("mastr_anlagen_eeg_biomasse")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_anlagen_eeg_wasser
_bronze_anlagen_eeg_wasser = read_bronze("mastr_anlagen_eeg_wasser")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_anlagen_eeg_geothermie_gsgk
_bronze_anlagen_eeg_geothermie_gsgk = read_bronze("mastr_anlagen_eeg_geothermie_gsgk")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_anlagen_kwk
_bronze_anlagen_kwk = read_bronze("mastr_anlagen_kwk")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_einheiten_genehmigung
_bronze_einheiten_genehmigung = read_bronze("mastr_einheiten_genehmigung")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_ertuechtigungen
_bronze_ertuechtigungen = read_bronze("mastr_ertuechtigungen")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- EEG tables for the support-unit bridge
_eeg_wind_bronze = read_bronze("mastr_anlagen_eeg_wind").select(
    "EegMaStRNummer", LINK_COL
)
_eeg_biomasse_bronze = read_bronze("mastr_anlagen_eeg_biomasse").select(
    "EegMaStRNummer", LINK_COL
)
_eeg_wasser_bronze = read_bronze("mastr_anlagen_eeg_wasser").select(
    "EegMaStRNummer", LINK_COL
)
_eeg_geothermie_gsgk_bronze = read_bronze("mastr_anlagen_eeg_geothermie_gsgk").select(
    "EegMaStRNummer", LINK_COL
)

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_anlagen_kwk (for the KWK bridge)
_kwk_bridge_bronze = read_bronze("mastr_anlagen_kwk")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_einheiten_genehmigung (for the authorisation bridge)
_gen_bridge_bronze = read_bronze("mastr_einheiten_genehmigung")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_ertuechtigungen (for the repowering bridge)
_ert_bridge_bronze = read_bronze("mastr_ertuechtigungen")

# COMMAND ----------

# DBTITLE 1,Transform -- mastr_anlagen_eeg_wind
_df_anlagen_eeg_wind = mastr_standardise(
    _bronze_anlagen_eeg_wind, NAME_MAP, CODED, source=SOURCE
)
_id_col_anlagen_eeg_wind = NAME_MAP.get("EegMaStRNummer", "EegMaStRNummer")
_df_anlagen_eeg_wind = _df_anlagen_eeg_wind.withColumn(
    "_srid", F.col(_id_col_anlagen_eeg_wind).cast("string")
)
_df_anlagen_eeg_wind = add_provenance(_df_anlagen_eeg_wind, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- mastr_anlagen_eeg_wind
write_silver(
    _df_anlagen_eeg_wind,
    "mastr_anlagen_eeg_wind",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect -- mastr_anlagen_eeg_wind
_findings_blocks = inspect_table(
    _df_anlagen_eeg_wind,
    "mastr_anlagen_eeg_wind",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=[_id_col_anlagen_eeg_wind],
    df_before=_bronze_anlagen_eeg_wind,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__mastr_anlagen_eeg_wind",
    "mastr_anlagen_eeg_wind",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- mastr_anlagen_eeg_biomasse
_df_anlagen_eeg_biomasse = mastr_standardise(
    _bronze_anlagen_eeg_biomasse, NAME_MAP, CODED, source=SOURCE
)
_id_col_anlagen_eeg_biomasse = NAME_MAP.get("EegMaStRNummer", "EegMaStRNummer")
_df_anlagen_eeg_biomasse = _df_anlagen_eeg_biomasse.withColumn(
    "_srid", F.col(_id_col_anlagen_eeg_biomasse).cast("string")
)
_df_anlagen_eeg_biomasse = add_provenance(
    _df_anlagen_eeg_biomasse, SOURCE, "_srid", RID
)

# COMMAND ----------

# DBTITLE 1,Write Silver -- mastr_anlagen_eeg_biomasse
write_silver(
    _df_anlagen_eeg_biomasse,
    "mastr_anlagen_eeg_biomasse",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect -- mastr_anlagen_eeg_biomasse
_findings_blocks = inspect_table(
    _df_anlagen_eeg_biomasse,
    "mastr_anlagen_eeg_biomasse",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=[_id_col_anlagen_eeg_biomasse],
    df_before=_bronze_anlagen_eeg_biomasse,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__mastr_anlagen_eeg_biomasse",
    "mastr_anlagen_eeg_biomasse",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- mastr_anlagen_eeg_wasser
_df_anlagen_eeg_wasser = mastr_standardise(
    _bronze_anlagen_eeg_wasser, NAME_MAP, CODED, source=SOURCE
)
_id_col_anlagen_eeg_wasser = NAME_MAP.get("EegMaStRNummer", "EegMaStRNummer")
_df_anlagen_eeg_wasser = _df_anlagen_eeg_wasser.withColumn(
    "_srid", F.col(_id_col_anlagen_eeg_wasser).cast("string")
)
_df_anlagen_eeg_wasser = add_provenance(_df_anlagen_eeg_wasser, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- mastr_anlagen_eeg_wasser
write_silver(
    _df_anlagen_eeg_wasser,
    "mastr_anlagen_eeg_wasser",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect -- mastr_anlagen_eeg_wasser
_findings_blocks = inspect_table(
    _df_anlagen_eeg_wasser,
    "mastr_anlagen_eeg_wasser",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=[_id_col_anlagen_eeg_wasser],
    df_before=_bronze_anlagen_eeg_wasser,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__mastr_anlagen_eeg_wasser",
    "mastr_anlagen_eeg_wasser",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- mastr_anlagen_eeg_geothermie_gsgk
_df_anlagen_eeg_geothermie_gsgk = mastr_standardise(
    _bronze_anlagen_eeg_geothermie_gsgk, NAME_MAP, CODED, source=SOURCE
)
_id_col_anlagen_eeg_geothermie_gsgk = NAME_MAP.get("EegMaStRNummer", "EegMaStRNummer")
_df_anlagen_eeg_geothermie_gsgk = _df_anlagen_eeg_geothermie_gsgk.withColumn(
    "_srid", F.col(_id_col_anlagen_eeg_geothermie_gsgk).cast("string")
)
_df_anlagen_eeg_geothermie_gsgk = add_provenance(
    _df_anlagen_eeg_geothermie_gsgk, SOURCE, "_srid", RID
)

# COMMAND ----------

# DBTITLE 1,Write Silver -- mastr_anlagen_eeg_geothermie_gsgk
write_silver(
    _df_anlagen_eeg_geothermie_gsgk,
    "mastr_anlagen_eeg_geothermie_gsgk",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect -- mastr_anlagen_eeg_geothermie_gsgk
_findings_blocks = inspect_table(
    _df_anlagen_eeg_geothermie_gsgk,
    "mastr_anlagen_eeg_geothermie_gsgk",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=[_id_col_anlagen_eeg_geothermie_gsgk],
    df_before=_bronze_anlagen_eeg_geothermie_gsgk,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__mastr_anlagen_eeg_geothermie_gsgk",
    "mastr_anlagen_eeg_geothermie_gsgk",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- mastr_anlagen_kwk
_df_anlagen_kwk = mastr_standardise(_bronze_anlagen_kwk, NAME_MAP, CODED, source=SOURCE)
_id_col_anlagen_kwk = NAME_MAP.get("KwkMastrNummer", "KwkMastrNummer")
_df_anlagen_kwk = _df_anlagen_kwk.withColumn(
    "_srid", F.col(_id_col_anlagen_kwk).cast("string")
)
_df_anlagen_kwk = add_provenance(_df_anlagen_kwk, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- mastr_anlagen_kwk
write_silver(
    _df_anlagen_kwk, "mastr_anlagen_kwk", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect -- mastr_anlagen_kwk
_findings_blocks = inspect_table(
    _df_anlagen_kwk,
    "mastr_anlagen_kwk",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=[_id_col_anlagen_kwk],
    df_before=_bronze_anlagen_kwk,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__mastr_anlagen_kwk",
    "mastr_anlagen_kwk",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- mastr_einheiten_genehmigung
_df_einheiten_genehmigung = mastr_standardise(
    _bronze_einheiten_genehmigung, NAME_MAP, CODED, source=SOURCE
)
_id_col_einheiten_genehmigung = NAME_MAP.get("GenMastrNummer", "GenMastrNummer")
_df_einheiten_genehmigung = _df_einheiten_genehmigung.withColumn(
    "_srid", F.col(_id_col_einheiten_genehmigung).cast("string")
)
_df_einheiten_genehmigung = add_provenance(
    _df_einheiten_genehmigung, SOURCE, "_srid", RID
)

# COMMAND ----------

# DBTITLE 1,Write Silver -- mastr_einheiten_genehmigung
write_silver(
    _df_einheiten_genehmigung,
    "mastr_einheiten_genehmigung",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect -- mastr_einheiten_genehmigung
_findings_blocks = inspect_table(
    _df_einheiten_genehmigung,
    "mastr_einheiten_genehmigung",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=[_id_col_einheiten_genehmigung],
    df_before=_bronze_einheiten_genehmigung,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__mastr_einheiten_genehmigung",
    "mastr_einheiten_genehmigung",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- mastr_ertuechtigungen
_df_ertuechtigungen = mastr_standardise(
    _bronze_ertuechtigungen, NAME_MAP, CODED, source=SOURCE
)
_id_col_ertuechtigungen = NAME_MAP.get("Id", "Id")
_df_ertuechtigungen = _df_ertuechtigungen.withColumn(
    "_srid", F.col(_id_col_ertuechtigungen).cast("string")
)
_df_ertuechtigungen = add_provenance(_df_ertuechtigungen, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- mastr_ertuechtigungen
write_silver(
    _df_ertuechtigungen,
    "mastr_ertuechtigungen",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect -- mastr_ertuechtigungen
_findings_blocks = inspect_table(
    _df_ertuechtigungen,
    "mastr_ertuechtigungen",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=[_id_col_ertuechtigungen],
    df_before=_bronze_ertuechtigungen,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__mastr_ertuechtigungen",
    "mastr_ertuechtigungen",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- mastr_eeg_support_unit_bridge (union the 4 EEG sources)
_eeg_union = (
    _eeg_wind_bronze.unionByName(_eeg_biomasse_bronze)
    .unionByName(_eeg_wasser_bronze)
    .unionByName(_eeg_geothermie_gsgk_bronze)
)

# COMMAND ----------

# DBTITLE 1,Write mastr_eeg_support_unit_bridge (additive)
_b1 = explode_link_bridge(
    _eeg_union,
    "EegMaStRNummer",
    LINK_COL,
    "mastr_eeg_support_unit_bridge",
    source=SOURCE,
    component=COMPONENT,
    bronze_table="mastr_anlagen_eeg_wind,mastr_anlagen_eeg_biomasse,mastr_anlagen_eeg_wasser,mastr_anlagen_eeg_geothermie_gsgk",
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect mastr_eeg_support_unit_bridge + export findings
_findings_blocks = inspect_table(
    _b1,
    "mastr_eeg_support_unit_bridge",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["parent_id", "linked_id"],
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__mastr_eeg_support_unit_bridge",
    "mastr_eeg_support_unit_bridge",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Write mastr_kwk_support_unit_bridge (additive)
_b2 = explode_link_bridge(
    _kwk_bridge_bronze,
    "KwkMastrNummer",
    LINK_COL,
    "mastr_kwk_support_unit_bridge",
    source=SOURCE,
    component=COMPONENT,
    bronze_table="mastr_anlagen_kwk",
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect mastr_kwk_support_unit_bridge + export findings
_findings_blocks = inspect_table(
    _b2,
    "mastr_kwk_support_unit_bridge",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["parent_id", "linked_id"],
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__mastr_kwk_support_unit_bridge",
    "mastr_kwk_support_unit_bridge",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Write mastr_authorisation_unit_bridge (additive)
_b3 = explode_link_bridge(
    _gen_bridge_bronze,
    "GenMastrNummer",
    LINK_COL,
    "mastr_authorisation_unit_bridge",
    source=SOURCE,
    component=COMPONENT,
    bronze_table="mastr_einheiten_genehmigung",
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect mastr_authorisation_unit_bridge + export findings
_findings_blocks = inspect_table(
    _b3,
    "mastr_authorisation_unit_bridge",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["parent_id", "linked_id"],
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__mastr_authorisation_unit_bridge",
    "mastr_authorisation_unit_bridge",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Write mastr_repowering_eeg_bridge (additive)
_b4 = explode_link_bridge(
    _ert_bridge_bronze,
    "Id",
    "EegMastrNummer",
    "mastr_repowering_eeg_bridge",
    source=SOURCE,
    component=COMPONENT,
    bronze_table="mastr_ertuechtigungen",
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect mastr_repowering_eeg_bridge + export findings
_findings_blocks = inspect_table(
    _b4,
    "mastr_repowering_eeg_bridge",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["parent_id", "linked_id"],
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__mastr_repowering_eeg_bridge",
    "mastr_repowering_eeg_bridge",
    _findings_blocks,
)
