# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- HONDA_CHANNEL_CATALOG
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** curate the Honda site's channel identity -- `synthetic`
# MAGIC field class, no Bronze source; Honda ships no device master file.
# MAGIC `channel_code` matches the column name each channel appears under in its
# MAGIC `honda_*` Silver table, which `dim_device` (Gold) resolves against.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../../_silver_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../../_silver_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "honda_iot"
COMPONENT = "silver/energy/_reference/honda_reference/01_honda_channel_catalog"
RID = run_id()

SITE_NAME = "Honda Research Institute Smart Building"

# COMMAND ----------

# DBTITLE 1,Curated channel catalog
# heating_p/w's own CHP_elec is dropped in Silver as a duplicate of
# electricity_p/w's CHP, so it is not listed separately here.
_CHANNELS = [
    # (channel_code, subsystem, measurement_type, description)
    ("total", "electricity", "power", "Total site electricity, power"),
    ("PV", "electricity", "power", "Photovoltaic generation, power"),
    ("CHP", "electricity", "power", "CHP unit electrical output, power"),
    ("total", "electricity", "energy", "Total site electricity, energy"),
    ("PV", "electricity", "energy", "Photovoltaic generation, energy"),
    ("CHP", "electricity", "energy", "CHP unit electrical output, energy"),
    ("total", "heating", "power", "Total site heating, power"),
    ("CHP_heat", "heating", "power", "CHP unit thermal output, power"),
    ("total", "heating", "energy", "Total site heating, energy"),
    ("CHP_heat", "heating", "energy", "CHP unit thermal output, energy"),
    ("total", "cooling", "power", "Total site cooling, power"),
    ("cool_elec", "cooling", "power", "Cooling system electrical draw, power"),
    ("total", "cooling", "energy", "Total site cooling, energy"),
    ("cool_elec", "cooling", "energy", "Cooling system electrical draw, energy"),
]

_schema = (
    "channel_code string, subsystem string, measurement_type string, description string"
)
catalog = spark.createDataFrame(_CHANNELS, _schema)
catalog = catalog.withColumn("site_name", F.lit(SITE_NAME))

# COMMAND ----------

# DBTITLE 1,Transform -- honda_channel_catalog (provenance)
catalog = catalog.withColumn(
    "_srid", sha_key("site_name", "subsystem", "measurement_type", "channel_code")
)
catalog = add_provenance(catalog, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- honda_channel_catalog
write_silver(
    catalog, "honda_channel_catalog", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect honda_channel_catalog + export findings
_findings_blocks = inspect_table(
    catalog,
    "honda_channel_catalog",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["site_name", "subsystem", "measurement_type", "channel_code"],
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__honda_channel_catalog",
    "honda_channel_catalog",
    _findings_blocks,
)