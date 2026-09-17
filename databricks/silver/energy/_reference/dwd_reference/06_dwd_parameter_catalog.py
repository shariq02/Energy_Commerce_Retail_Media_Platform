# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- DWD_PARAMETER_CATALOG
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** the derived source-code -> business-name -> physical-unit catalog, built from `mappings/dwd.yml`'s parameter business names plus `dwd_parameter_unit` Bronze (unit + German description text). One of 7 sibling notebooks in this folder, split
# MAGIC from a single `01_dwd_reference.py` -- see the folder's other files for
# MAGIC the rest. Runs before the DWD measurement notebooks.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../../_silver_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../../_silver_inspect

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "dwd"
COMPONENT = "silver/energy/_reference/dwd_reference/06_dwd_parameter_catalog"
RID = run_id()

MAPPING = load_mapping(SOURCE)

# COMMAND ----------

# DBTITLE 1,Read Bronze -- dwd_parameter_unit (unit + description text)
_pu_bronze = read_bronze("dwd_parameter_unit")

# COMMAND ----------

# DBTITLE 1,Transform -- dwd_parameter_catalog (code -> business name -> unit)
param_bn = (MAPPING.get("business_names", {}) or {}).get("parameters", {})
bn_df = spark.createDataFrame(
    [(c, s["business_name"], s.get("unit")) for c, s in param_bn.items()],
    "parameter_source_code string, parameter_business_name string, mapped_unit string",
)
pu_units = (
    _pu_bronze.select(
        F.trim(F.col("Parameter")).alias("parameter_source_code"),
        F.trim(F.col("Einheit")).alias("parameter_unit"),
        F.trim(F.col("Parameterbeschreibung")).alias("parameter_description_de"),
    )
    .filter(F.col("parameter_source_code").isNotNull())
    .dropDuplicates(["parameter_source_code"])
)
cat = (
    bn_df.join(pu_units, "parameter_source_code", "left")
    .withColumn("parameter_unit", F.coalesce("parameter_unit", "mapped_unit"))
    .withColumn("catalog_vintage", F.lit("dwd_hourly_historical_20260904"))
    .drop("mapped_unit")
)

# COMMAND ----------

# DBTITLE 1,Write Silver -- dwd_parameter_catalog
write_silver(cat, "dwd_parameter_catalog", source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect dwd_parameter_catalog + export findings
_findings_blocks = inspect_table(
    cat,
    "dwd_parameter_catalog",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["parameter_source_code"],
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__dwd_parameter_catalog",
    "dwd_parameter_catalog",
    _findings_blocks,
)
