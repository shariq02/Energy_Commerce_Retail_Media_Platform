# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- MASTR_EINHEITEN_KERNKRAFT
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `mastr_einheiten_kernkraft` into source-scoped Silver at its unit grain
# MAGIC (`EinheitMastrNummer`) -- catalog decode, English business names,
# MAGIC capacities in kW, coordinates unchanged (out-of-Germany points recorded
# MAGIC to quarantine), AGS from the Gemeindeschluessel prefix. One of 6 sibling
# MAGIC notebooks in this folder, split from a single
# MAGIC `01_mastr_generation_units.py` -- see the folder's other files for the
# MAGIC other 5 carriers. Runs after `../../_reference/mastr_reference_catalogs/`.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../../_silver_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../../_silver_inspect

# COMMAND ----------

# DBTITLE 1,MaStR generation units shared helpers
# MAGIC %run ./_mastr_generation_units_common

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "mastr"
COMPONENT = (
    "silver/energy/generation/mastr_generation_units/05_mastr_einheiten_kernkraft"
)
RID = run_id()

MAPPING = load_mapping(SOURCE)
NAME_MAP = flatten_business_names(MAPPING, SOURCE)
CODED = coded_columns(MAPPING, SOURCE)

# Drop only columns constant BY CONSTRUCTION (single-carrier tables,
# German-only register). Other flagged constants may be incidental -- kept.
STRUCTURAL_CONSTANT_COLS = ["Energietraeger", "Land"]

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_einheiten_kernkraft
_bronze = read_bronze("mastr_einheiten_kernkraft")

# COMMAND ----------

# DBTITLE 1,Transform -- mastr_einheiten_kernkraft
df = mastr_standardise(_bronze, NAME_MAP, CODED, source=SOURCE)
df = _drop_structural_constants(df)

quarantine_out_of_bbox(df, "mastr_einheiten_kernkraft")

df = attach_ags_prefix(df, "municipality_key_ags")
df = df.withColumn("_srid", F.col("unit_id").cast("string"))
df = add_provenance(df, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- mastr_einheiten_kernkraft
write_silver(
    df, "mastr_einheiten_kernkraft", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect -- mastr_einheiten_kernkraft
_findings_blocks = inspect_table(
    df,
    "mastr_einheiten_kernkraft",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["unit_id"],
    df_before=_bronze,
    extra_checks={
        "structural_constants_dropped": ",".join(
            c
            for c in STRUCTURAL_CONSTANT_COLS
            if NAME_MAP.get(c, c) not in df.columns and c not in df.columns
        )
    },
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__mastr_einheiten_kernkraft",
    "mastr_einheiten_kernkraft",
    _findings_blocks,
)
