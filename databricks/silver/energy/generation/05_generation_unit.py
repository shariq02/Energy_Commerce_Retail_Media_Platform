# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- GENERATION UNIT (MASTR)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** the six MaStR technology unit tables as one `generation_unit`
# MAGIC structure with `unit_type`; unit ids are unique across technologies.

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

# DBTITLE 1,MaStR unit helpers (structural constants, bounding-box quarantine)
# MAGIC %run ./mastr_generation_units/_mastr_generation_units_common

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "mastr"
COMPONENT = "silver/energy/generation/05_generation_unit"
RID = run_id()
FINDINGS = "mastr"
TABLE = "generation_unit"
MAPPING = load_mapping(SOURCE)
NAME_MAP = flatten_business_names(MAPPING, SOURCE)
CODED = coded_columns(MAPPING, SOURCE)
# Constant by construction in each single-technology table.
STRUCTURAL_CONSTANT_COLS = ["Energietraeger", "Land"]
UNIT_TYPES = (
    "wind",
    "biomasse",
    "wasser",
    "verbrennung",
    "kernkraft",
    "geothermie_gsgk",
)

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_einheiten_{unit type}
bronze = {u: read_bronze(f"mastr_einheiten_{u}") for u in UNIT_TYPES}

# COMMAND ----------

# DBTITLE 1,Transform -- standardise each technology and union with unit_type
units = None
for unit_type, df in bronze.items():
    bt = f"mastr_einheiten_{unit_type}"
    df = _drop_structural_constants(mastr_standardise(df, NAME_MAP, CODED))
    quarantine_out_of_bbox(df, bt)
    df = (
        attach_ags_prefix(df, "source_municipality_key")
        .withColumn("unit_type", F.lit(unit_type))
        .withColumn("source_dataset", F.lit(bt))
    )
    units = df if units is None else units.unionByName(df, allowMissingColumns=True)
units = units.withColumn("_srid", sha_key("unit_type", "unit_id"))
units = add_semantic_provenance(units, SOURCE, None, RID, "_srid")

# COMMAND ----------

# DBTITLE 1,Write Silver -- generation_unit
write_semantic(units, TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect -- generation_unit
written = spark.table(semantic_table(TABLE))
findings_blocks = inspect_table(
    written,
    TABLE,
    source=FINDINGS,
    component=COMPONENT,
    rid=RID,
    key_cols=["unit_id"],
    extra_checks={
        "rows_by_unit_type": {
            r["unit_type"]: r["count"]
            for r in written.groupBy("unit_type").count().collect()
        }
    },
)

# COMMAND ----------

# DBTITLE 1,Export findings -- generation_unit
write_silver_findings(
    FINDINGS, f"{COMPONENT.split('/')[-1]}__{TABLE}", TABLE, findings_blocks
)
