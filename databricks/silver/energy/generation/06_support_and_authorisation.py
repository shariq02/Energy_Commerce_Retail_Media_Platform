# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- SUPPORT REGISTRATION, AUTHORISATION, REPOWERING (MASTR)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** Renewable-energy-act (four technologies) and combined-heat-and-power
# MAGIC registrations as one `support_registration` (`support_scheme`, `unit_type`; the id spaces are
# MAGIC disjoint); unit authorisations and repowering as their own structures.

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
COMPONENT = "silver/energy/generation/06_support_and_authorisation"
RID = run_id()
FINDINGS = "mastr"
MAPPING = load_mapping(SOURCE)
NAME_MAP = flatten_business_names(MAPPING, SOURCE)
CODED = coded_columns(MAPPING, SOURCE)
# Bronze table -> (support_scheme, unit_type, registration id column)
SUPPORT = {
    "mastr_anlagen_eeg_wind": ("renewable_energy_act", "wind", "EegMaStRNummer"),
    "mastr_anlagen_eeg_biomasse": (
        "renewable_energy_act",
        "biomasse",
        "EegMaStRNummer",
    ),
    "mastr_anlagen_eeg_wasser": ("renewable_energy_act", "wasser", "EegMaStRNummer"),
    "mastr_anlagen_eeg_geothermie_gsgk": (
        "renewable_energy_act",
        "geothermie_gsgk",
        "EegMaStRNummer",
    ),
    "mastr_anlagen_kwk": ("combined_heat_and_power", None, "KwkMastrNummer"),
}
# structure -> (Bronze table, id column)
SINGLE = {
    "unit_authorisation": ("mastr_einheiten_genehmigung", "GenMastrNummer"),
    "unit_repowering": ("mastr_ertuechtigungen", "Id"),
}

# COMMAND ----------

# DBTITLE 1,Read Bronze -- EEG and KWK registrations
support_bronze = {bt: read_bronze(bt) for bt in SUPPORT}

# COMMAND ----------

# DBTITLE 1,Read Bronze -- authorisations and repowering
single_bronze = {name: read_bronze(bt) for name, (bt, _) in SINGLE.items()}

# COMMAND ----------

# DBTITLE 1,Transform -- support_registration
registrations = None
for bt, (scheme, unit_type, id_col) in SUPPORT.items():
    df = mastr_standardise(support_bronze[bt], NAME_MAP, CODED)
    df = (
        df.withColumn(
            "support_registration_id",
            F.col(NAME_MAP.get(id_col, id_col)).cast("string"),
        )
        .withColumn("support_scheme", F.lit(scheme))
        .withColumn("unit_type", F.lit(unit_type).cast("string"))
        .withColumn("source_dataset", F.lit(bt))
    )
    registrations = (
        df
        if registrations is None
        else registrations.unionByName(df, allowMissingColumns=True)
    )
registrations = registrations.withColumn(
    "_srid", sha_key("support_scheme", "support_registration_id")
)
registrations = add_semantic_provenance(registrations, SOURCE, None, RID, "_srid")

# COMMAND ----------

# DBTITLE 1,Transform -- unit_authorisation and unit_repowering
singles = {}
for name, (bt, id_col) in SINGLE.items():
    names = {**NAME_MAP, **MASTR_KEY_NAMES.get(bt, {})}
    df = mastr_standardise(single_bronze[name], names, CODED)
    df = df.withColumn("_srid", F.col(names.get(id_col, id_col)).cast("string"))
    singles[name] = add_semantic_provenance(df, SOURCE, bt, RID, "_srid")

# COMMAND ----------

# DBTITLE 1,Configuration -- structure -> frame
STRUCTURES = {"support_registration": registrations, **singles}

# COMMAND ----------

# DBTITLE 1,Write Silver -- support, authorisation, repowering
for name, frame in STRUCTURES.items():
    write_semantic(frame, name, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect -- support, authorisation, repowering
findings_blocks = {
    name: inspect_table(
        spark.table(semantic_table(name)),
        name,
        source=FINDINGS,
        component=COMPONENT,
        rid=RID,
        key_cols=["source_record_id"],
    )
    for name in STRUCTURES
}

# COMMAND ----------

# DBTITLE 1,Export findings -- support, authorisation, repowering
for name, blocks in findings_blocks.items():
    write_silver_findings(FINDINGS, f"{COMPONENT.split('/')[-1]}__{name}", name, blocks)
